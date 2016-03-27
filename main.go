package main

import (
	"flag"
	"os"
	"runtime"
	"runtime/pprof"

	// Backend Side-Effect imports
	_ "github.com/dataux/dataux/backends/datastore"
	_ "github.com/dataux/dataux/backends/elasticsearch"
	_ "github.com/dataux/dataux/backends/files"
	_ "github.com/dataux/dataux/backends/mongo"
	// Frontend's side-effect imports
	_ "github.com/dataux/dataux/frontends/mysqlfe"

	u "github.com/araddon/gou"
	"github.com/dataux/dataux/proxy"
)

var (
	configFile     string
	cpuProfileFile string
	memProfileFile string
	logLevel       = "info"
)

func init() {
	flag.StringVar(&configFile, "config", "dataux.conf", "dataux proxy config file")
	flag.StringVar(&logLevel, "loglevel", "debug", "logging [ debug,info,warn,error ]")
	flag.StringVar(&cpuProfileFile, "cpuprofile", "", "cpuprofile")
	flag.StringVar(&memProfileFile, "memprofile", "", "memProfileFile")
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

	if cpuProfileFile != "" {
		f, err := os.Create(cpuProfileFile)
		if err != nil {
			u.Errorf("could not care cpu file: %v", err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	proxy.LoadConfig(configFile)

	proxy.RunDaemon(true, 2)
}
