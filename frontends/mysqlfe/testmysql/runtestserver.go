package testmysql

import (
	"sync"
	"testing"
	"time"

	u "github.com/araddon/gou"
	"github.com/bmizerany/assert"
	"github.com/lytics/grid/natsunit"
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
	conf, err := models.LoadConfig(TestConfigData)
	if err != nil {
		panic("must load confiig")
	}
	Conf = conf
}
func SchemaLoader(name string) (*schema.Schema, error) {
	//u.Infof("SchemaLoader")
	return Schema, nil
}

var TestConfigData = `

supress_recover: true

# etcd = [ ]
# etcd server list dynamically created and injected

nats  = [ "nats://127.0.0.1:9547" ]

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
    sources : [ "mgo_datauxtest", "es_test", "localfiles" , "google_ds_test"]
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
  }
  {
    name : es_test
    type : elasticsearch
  }
  
  {
    name     : localfiles
    type     : cloudstore
    settings : {
      type             : localfs
      path             : "tables/"
      format           : "csv"
    }
  }

  # this section is for http://seanlahman.com/baseball-archive/statistics/
  # csv files 
  #  must have TESTINT=true integration test flag turned on
  {
    name     : baseball
    type     : cloudstore
    settings : {
      type             : gcs
      bucket           : "lytics-dataux-tests"
      path             : "baseball/"
      format           : "csv"
    }
  }
  
  # this is the google-datastore database config
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

func NewTestServerForDb(t *testing.T, db string) {
	f := func() {

		assert.Tf(t, Conf != nil, "must load config without err: %v", Conf)

		EtcdCluster = embeddedetcd.TestClusterOf1()
		EtcdCluster.Launch()
		etcdServers := EtcdCluster.HTTPMembers()[0].ClientURLs
		//u.Infof("etcdServers: %#v", etcdServers)
		Conf.Etcd = etcdServers

		natsunit.StartEmbeddedNATS()

		planner.GridConf.EtcdServers = etcdServers

		ServerCtx = models.NewServerCtx(Conf)
		ServerCtx.Init()
		go func() {
			ServerCtx.Grid.RunMaster()
		}()

		Schema, _ = ServerCtx.Schema(db)
		//u.Infof("starting %q schema in test", db)

		svr, err := proxy.NewServer(ServerCtx)
		assert.T(t, err == nil, "must start without error ", err)

		go svr.Run()

		// delay to ensure we have time to connect
		time.Sleep(100 * time.Millisecond)
	}

	testServerOnce.Do(f)
}

func NewTestServer(t *testing.T) {
	NewTestServerForDb(t, "datauxtest")
}

func RunTestServer(t *testing.T) {
	NewTestServer(t)
}
