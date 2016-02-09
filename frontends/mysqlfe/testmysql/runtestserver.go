package testmysql

import (
	"sync"
	"testing"
	"time"

	u "github.com/araddon/gou"
	"github.com/bmizerany/assert"
	"github.com/lytics/sereno/embeddedetcd"

	"github.com/araddon/qlbridge/schema"
	"github.com/dataux/dataux/frontends/mysqlfe"
	"github.com/dataux/dataux/models"
	"github.com/dataux/dataux/planner"
	"github.com/dataux/dataux/vendored/mixer/client"
	mysqlproxy "github.com/dataux/dataux/vendored/mixer/proxy"
)

var (
	_              = u.EMPTY
	testServerOnce sync.Once
	testListener   *TestListenerWraper
	testDBOnce     sync.Once
	testDB         *client.DB
	Conf           *models.Config
	EtcdCluster    *embeddedetcd.EtcdCluster
	ServerCtx      *models.ServerCtx
	Schema         *schema.Schema
)

func init() {
	conf, err := models.LoadConfig(testConfigData)
	if err != nil {
		panic("must load confiig")
	}
	Conf = conf
}
func SchemaLoader(name string) (*schema.Schema, error) {
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
    sources : [ "mgo_datauxtest", "es_test", "csvlocal" , "google_ds_test"]
  }
]

# sources
sources : [
  {
    name : mgo_datauxtest
    type : mongo
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

type TestListenerWraper struct {
	*mysqlproxy.MysqlListener
}

func NewTestServer(t *testing.T) *TestListenerWraper {
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

		Schema = ServerCtx.Schema("datauxtest")

		handler, err := mysqlfe.NewMySqlHandler(ServerCtx)
		assert.Tf(t, err == nil, "must create es handler without err: %v", err)

		// Load our Frontend Listener's
		models.ListenerRegister(mysqlproxy.ListenerType,
			mysqlproxy.ListenerInit,
			handler,
		)

		myl, err := mysqlproxy.NewMysqlListener(Conf.Frontends[0], Conf)
		assert.Tf(t, err == nil, "must create listener without err: %v", err)

		testListener = &TestListenerWraper{myl}

		go testListener.Run(handler, make(chan bool))

		// delay to ensure we have time to connect
		time.Sleep(100 * time.Millisecond)
	}

	testServerOnce.Do(f)

	return testListener
}

func RunTestServer(t *testing.T) {
	NewTestServer(t)
}
