package main

import (
	"flag"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"

	// Backend Side-Effect imports
	_ "github.com/dataux/dataux/backends/bigtable"
	_ "github.com/dataux/dataux/backends/cassandra"
	_ "github.com/dataux/dataux/backends/datastore"
	_ "github.com/dataux/dataux/backends/elasticsearch"
	_ "github.com/dataux/dataux/backends/files"
	_ "github.com/dataux/dataux/backends/kubernetes"
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
	logLevel   = "info"
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

	if len(configFile) == 0 {
		u.Errorf("must use a config file")
		return
	}
	u.SetupLogging(logLevel)
	u.SetColorIfTerminal()

	proxy.LoadConfig(configFile)

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
