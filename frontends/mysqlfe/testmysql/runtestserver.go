package testmysql

import (
	"os"
	"sync"
	"testing"
	"time"

	u "github.com/araddon/gou"
	"github.com/bmizerany/assert"
	"github.com/coreos/etcd/embed"
	"github.com/lytics/grid/grid.v2/natsunit"

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
    sources : [ 
      "mgo_datauxtest", 
      "es_test", 
      "localfiles", 
      "google_ds_test", 
      "cass", 
      "bt",
      "kube"
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

		// EtcdCluster = embeddedetcd.TestClusterOf1()
		// EtcdCluster.Launch()
		// etcdServers := EtcdCluster.HTTPMembers()[0].ClientURLs
		//u.Infof("etcdServers: %#v", etcdServers)
		//Conf.Etcd = etcdServers

		// etcd.embed package for some reason uses non localhost ip's
		embed.DefaultInitialAdvertisePeerURLs = "http://127.0.0.1:2380"
		embed.DefaultAdvertiseClientURLs = "http://127.0.0.1:2379"

		cfg := embed.NewConfig()
		os.RemoveAll("test.etcd")
		cfg.Dir = "test.etcd"
		e, err := embed.StartEtcd(cfg)
		if err != nil {
			panic(err.Error())
		}
		if e == nil {
			panic("must have etcd server")
		}
		//defer e.Close()

		Conf.Etcd = []string{embed.DefaultAdvertiseClientURLs}
		//Conf.Etcd = []string{"http://127.0.0.1:2379"}

		_, err = natsunit.StartEmbeddedNATS()
		if err != nil {
			panic(err.Error)
		} else {
			u.Debugf("started embedded nats %v", natsunit.TestURL)
		}

		planner.GridConf.EtcdServers = Conf.Etcd

		ServerCtx = models.NewServerCtx(Conf)
		//u.Infof("init")
		ServerCtx.Init()
		//u.Infof("after init")
		quit := make(chan bool)
		go func() {
			ServerCtx.PlanGrid.Run(quit)
		}()

		Schema, _ = ServerCtx.Schema(db)
		u.Infof("starting %q schema in test", db)

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
