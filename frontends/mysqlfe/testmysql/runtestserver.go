package testmysql

import (
	"fmt"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	u "github.com/araddon/gou"
	"github.com/coreos/etcd/embed"
	"github.com/coreos/pkg/capnslog"

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
	return Schema, nil
}

var TestConfigData = `

supress_recover: true

# etcd = [ ]
# etcd server list dynamically created and injected

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
    sources : [ 
      "mgo_datauxtest", 
      "es_test", 
      "localfiles", 
      "google_ds_test", 
      "cass", 
      "bt",
      "bigquery",
      "kube",
      "lytics"
    ]
  }
]

# sources
sources : [

  {
    type : mongo
    name : mgo_datauxtest
    # partitions describe how to break up 
    # queries across nodes if multi-node db, this 
    # is single node so just used for unit tests to simulate multi-node
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
    name : cass
    type : cassandra
    settings {
      keyspace  "datauxtest"
      hosts    ["localhost:9042"]
    }
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
      localpath        : "tables/"
      format           : "csv"
    }
  }

  # csv-file "db" of data from http://seanlahman.com/baseball-archive/statistics/
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
  
  # google-datastore database config
  {
    name : google_ds_test
    type : google-datastore
  }

  {
    name : mysql_test
    type : mysql
  }

  {
    name : kube
    type : kubernetes
  }

  {
    name : bt
    type : bigtable
    tables_to_load : [ "datauxtest" , "article", "user", "event" ]
    settings {
      instance  "bigtable0"
      # project will be loaded from ENV   $GCEPROJECT
    }
  }

  {
    name : bigquery
    type : bigquery
    settings {
      # project will be loaded from ENV   $GCEPROJECT
      billing_project : ""
      data_project : "bigquery-public-data"
      dataset : "san_francisco"
    }
  }

  {
    name : lytics
    type : lytics
    settings {

    }
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

func EtcdConfig() *embed.Config {

	// Cleanup
	os.RemoveAll("test.etcd")
	os.RemoveAll(".test.etcd")
	os.RemoveAll("/tmp/test.etcd")

	embed.DefaultInitialAdvertisePeerURLs = "http://127.0.0.1:22380"
	embed.DefaultAdvertiseClientURLs = "http://127.0.0.1:22379"

	cfg := embed.NewConfig()

	lpurl, _ := url.Parse("http://localhost:22380")
	lcurl, _ := url.Parse("http://localhost:22379")
	cfg.LPUrls = []url.URL{*lpurl}
	cfg.LCUrls = []url.URL{*lcurl}

	cfg.Dir = "/tmp/test.etcd"

	return cfg
}
func NewTestServerForDb(t *testing.T, db string) {
	startServer(db)
}
func StartServer() {
	startServer("datauxtest")
}
func startServer(db string) {
	f := func() {

		if Conf == nil {
			panic("Must have Conf")
		}

		capnslog.SetGlobalLogLevel(capnslog.CRITICAL)

		e, err := embed.StartEtcd(EtcdConfig())
		if err != nil {
			panic(err.Error())
		}
		if e == nil {
			panic("must have etcd server")
		}
		// can't defer close as this function returns immediately
		//defer e.Close()

		Conf.Etcd = []string{embed.DefaultAdvertiseClientURLs}

		planner.GridConf.EtcdServers = Conf.Etcd

		ServerCtx = models.NewServerCtx(Conf)
		ServerCtx.Init()
		quit := make(chan bool)
		go func() {
			ServerCtx.PlanGrid.Run(quit)
		}()

		time.Sleep(time.Millisecond * 20)

		Schema, _ = ServerCtx.Schema(db)
		u.Infof("starting %q schema in test", db)

		svr, err := proxy.NewServer(ServerCtx)
		if err != nil {
			panic(fmt.Sprintf("must not have error %v", err))
		}

		go svr.Run()

		u.Debugf("starting server")

		// delay to ensure we have time to connect
		time.Sleep(1000 * time.Millisecond)
	}

	testServerOnce.Do(f)
}

func NewTestServer(t *testing.T) {
	startServer("datauxtest")
}

func RunTestServer(t *testing.T) {
	startServer("datauxtest")
}
