package testmysql

import (
	"flag"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/expr/builtins"
	"github.com/araddon/qlbridge/schema"

	// Frontend's side-effect imports
	_ "github.com/dataux/dataux/frontends/mysqlfe"

	"github.com/dataux/dataux/models"
	"github.com/dataux/dataux/planner"
	"github.com/dataux/dataux/proxy"
	"github.com/dataux/dataux/vendored/mixer/client"
)

var (
	testServerOnce sync.Once
	testDBOnce     sync.Once
	testDB         *client.DB
	Conf           *models.Config
	ServerCtx      *models.ServerCtx
	Schema         *schema.Schema
	verbose        *bool
	setupOnce      = sync.Once{}
)

func init() {
	conf, err := models.LoadConfig(TestConfigData)
	if err != nil {
		panic("must load confiig")
	}
	Conf = conf
}

// SchemaLoader is a function for teting only to have Schema available as global
func SchemaLoader(name string) (*schema.Schema, error) {
	if Schema == nil {
		u.Errorf("no schema")
	}
	return Schema, nil
}

// Setup enables -vv verbose logging or sends logs to /dev/null
// env var VERBOSELOGS=true was added to support verbose logging with alltests
func Setup() {
	setupOnce.Do(func() {

		if flag.CommandLine.Lookup("vv") == nil {
			verbose = flag.Bool("vv", false, "Verbose Logging?")
		}

		flag.Parse()
		logger := u.GetLogger()
		if logger != nil {
			// don't re-setup
		} else {
			if (verbose != nil && *verbose == true) || os.Getenv("VERBOSELOGS") != "" {
				u.SetupLogging("debug")
				u.SetColorOutput()
			} else {
				// make sure logging is always non-nil
				dn, _ := os.Open(os.DevNull)
				u.SetLogger(log.New(dn, "", 0), "error")
			}
		}
		builtins.LoadAllBuiltins()
	})
}

var TestConfigData = `

supress_recover = true

etcd = [ "http://127.0.0.1:2379" ]

frontends [
  {
    type : mysql
    address : "127.0.0.1:13307"
  }
]
`

// TestingT is an interface wrapper around *testing.T so when we import
// this go dep, govendor don't import "testing"
type TestingT interface {
	Errorf(format string, args ...interface{})
}

func NewTestServerForDb(t TestingT, db string) {
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

		planner.GridConf.EtcdServers = Conf.Etcd
		//u.Infof("etcd hosts: %v", planner.GridConf.EtcdServers)

		ServerCtx = models.NewServerCtx(Conf)
		ServerCtx.Init()
		quit := make(chan bool)
		go func() {
			ServerCtx.PlanGrid.Run(quit)
		}()

		time.Sleep(time.Millisecond * 20)

		Schema, _ = ServerCtx.Schema(db)
		//u.Infof("starting %q schema in test", db)

		svr, err := proxy.NewServer(ServerCtx)
		if err != nil {
			panic(fmt.Sprintf("must not have error %v", err))
		}

		go svr.Run()

		// delay to ensure we have time to connect
		time.Sleep(1000 * time.Millisecond)
	}

	testServerOnce.Do(f)
}

func NewTestServer(t TestingT) {
	startServer("datauxtest")
}

func RunTestServer(t TestingT) {
	startServer("datauxtest")
}
