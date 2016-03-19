package testmysql

import (
	"sync"
	"testing"
	"time"

	u "github.com/araddon/gou"
	"github.com/bmizerany/assert"
	"github.com/lytics/sereno/embeddedetcd"

	// Frontend's side-effect imports
	_ "github.com/dataux/dataux/frontends/mysqlfe"

	"github.com/araddon/qlbridge/schema"
	"github.com/dataux/dataux/models"
	"github.com/dataux/dataux/planner"
	"github.com/dataux/dataux/proxy"
	"github.com/dataux/dataux/vendored/mixer/client"
)

var (
	_              = u.EMPTY
	testServerOnce sync.Once
	testDBOnce     sync.Once
	testDB         *client.DB
	Conf           *models.Config
	EtcdCluster    *embeddedetcd.EtcdCluster
	ServerCtx      *models.ServerCtx
	Schema         *schema.Schema
)

func init() {
	u.SetupLogging("debug")
	u.SetColorOutput()
	conf, err := models.LoadConfig(testConfigData)
	if err != nil {
		panic("must load confiig")
	}
	Conf = conf
}
func SchemaLoader(name string) (*schema.Schema, error) {
	u.Infof("SchemaLoader")
	return Schema, nil
}

var testConfigData = `

supress_recover: true

frontends [
  {
    type : mysql
    address : "127.0.0.1:13307"
  }
]

# schemas
schemas : [
  {
    name : datauxtest
    sources : [ "mgo_datauxtest", "es_test", "gcscsvs", "csvlocal" , "google_ds_test"]
  }
]

# sources
sources : [
  {
    name : mgo_datauxtest
    type : mongo
    partitions : [
        {
            table : article
            keys : [ "title"]
            partitions : [
               {
                   id    : a
                   right : m
               },
               {
                   id    : b
                   left  : m
               }
            ]
        }
    ]
  },
  {
    name : es_test
    type : elasticsearch
  },
  {
    name : csvlocal
    type : csv
  },
  {
    # this section is for http://seanlahman.com/baseball-archive/statistics/
    # csv files
    name     : gcscsvs
    type     : cloudstore
    settings : {
      type             : gcs
      gcsbucket        : "lytics-dataux-tests"
      path             : "tables/"
      format           : "csv"
    }
  },
  {
    name     : gcscsvs2
    type     : cloudstore
    settings : {
      type             : gcs
      gcsbucket        : "lytics-dataux-tests"
      path             : "baseball/"
      format           : "csv"
    }
  },
  {
    name : google_ds_test
    type : google-datastore
  },
  {
    name : mysql_test
    type : mysql
  }
]

# List of nodes hosting data sources
nodes : [
  {
    name    : estest1
    source  : es_test
    address : "http://localhost:9200"
  },
  {
    name    : mgotest1
    source  : mgo_datauxtest
    address : "localhost"
  },
  {
    name    : csvlocal1
    source  : csvlocal
    address : "$GOPATH/src/github.com/dataux/dataux/data"
  },
  {
    name    : googleds1
    source  : google_ds_test
    address : "$GOOGLEJWT"
  }
]

`

func NewTestServer(t *testing.T) {
	f := func() {

		assert.Tf(t, Conf != nil, "must load config without err: %v", Conf)

		EtcdCluster = embeddedetcd.TestClusterOf1()
		EtcdCluster.Launch()
		etcdServers := EtcdCluster.HTTPMembers()[0].ClientURLs
		//u.Infof("etcdServers: %#v", etcdServers)
		Conf.Etcd = etcdServers

		planner.GridConf.EtcdServers = etcdServers

		ServerCtx = models.NewServerCtx(Conf)
		ServerCtx.Init()

		Schema, _ = ServerCtx.Schema("datauxtest")

		svr, err := proxy.NewServer(ServerCtx)
		assert.T(t, err == nil, "must start without error ", err)

		go svr.Run()

		// delay to ensure we have time to connect
		time.Sleep(100 * time.Millisecond)
	}

	testServerOnce.Do(f)
}

func RunTestServer(t *testing.T) {
	NewTestServer(t)
}
