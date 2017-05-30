package main

import (
	"flag"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"

	// Backend Side-Effect imports, ie load the providers into registry but
	// config will determine if they get used.
	// if you are building custom daemon, you can cherry pick sources you care about
	_ "github.com/araddon/qlbridge/datasource/files"
	_ "github.com/dataux/dataux/backends/bigquery"
	_ "github.com/dataux/dataux/backends/bigtable"
	_ "github.com/dataux/dataux/backends/cassandra"
	_ "github.com/dataux/dataux/backends/datastore"
	_ "github.com/dataux/dataux/backends/elasticsearch"
	_ "github.com/dataux/dataux/backends/kubernetes"
	_ "github.com/dataux/dataux/backends/lytics"
	_ "github.com/dataux/dataux/backends/mongo"

	// Frontend's side-effect imports
	_ "github.com/dataux/dataux/frontends/mysqlfe"

	u "github.com/araddon/gou"
	"github.com/dataux/dataux/proxy"
)

var (
	configFile string
	pprofPort  string
	workerCt   int
	logLevel   = "debug"
)

func init() {
	flag.StringVar(&configFile, "config", "dataux.conf", "dataux proxy config file")
	flag.StringVar(&logLevel, "loglevel", "debug", "logging [ debug,info,warn,error ]")
	flag.StringVar(&pprofPort, "pprof", ":18008", "pprof and metrics port")
	flag.IntVar(&workerCt, "workerct", 3, "Number of worker nodes")
	flag.Parse()
}
func main() {

	runtime.GOMAXPROCS(runtime.NumCPU())
	u.SetupLogging(logLevel)
	u.SetColorIfTerminal()

	// First try to look for dataux.conf or provided conf file
	// if that fails then use the empty default which means api's
	// etc can be used to dynamically define schema etc
	_, err := proxy.LoadConfig(configFile)
	if err != nil {
		_, err = proxy.LoadConfigString(DefaultConfig)
		if err != nil {
			os.Exit(1)
		}
	}

	// go profiling
	if pprofPort != "" {
		conn, err := net.Listen("tcp", pprofPort)
		if err != nil {
			u.Warnf("Error listening on %s: %v", pprofPort, err)
			os.Exit(1)
		}
		go func() {
			if err := http.Serve(conn, http.DefaultServeMux); err != nil {
				u.Errorf("Error from profile HTTP server: %v", err)
			}
			conn.Close()
		}()
	}

	proxy.RunDaemon(true, workerCt)
}

var DefaultConfig = `

frontends : [
  {
    type    : mysql
    address : "0.0.0.0:4000"
  }
]

`
