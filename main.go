package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"syscall"

	// Backend Registrations Side-Effect imports
	_ "github.com/dataux/dataux/backends/elasticsearch"
	_ "github.com/dataux/dataux/backends/mongo"

	u "github.com/araddon/gou"
	"github.com/dataux/dataux/frontends/mysqlfe"
	"github.com/dataux/dataux/models"
	"github.com/dataux/dataux/proxy"
	mysqlproxy "github.com/dataux/dataux/vendored/mixer/proxy"
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

	// get config
	conf, err := models.LoadConfigFromFile(configFile)
	if err != nil {
		u.Errorf("Could not load config: %v", err)
		os.Exit(1)
	}
	// Make Server Context
	svrCtx := models.NewServerCtx(conf)
	svrCtx.Init()

	// TODO:   these should be started by server through registry, imports
	mysqlHandler, err := mysqlfe.NewMySqlHandler(svrCtx)
	if err != nil {
		u.Errorf("Could not create handlers: %v", err)
		os.Exit(1)
	}

	// Load our Frontend Listener's
	models.ListenerRegister(mysqlproxy.ListenerType,
		mysqlproxy.ListenerInit,
		mysqlHandler,
	)

	var svr *proxy.Server
	svr, err = proxy.NewServer(svrCtx)
	if err != nil {
		u.Errorf("%v", err)
		return
	}

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	go func() {
		sig := <-sc
		u.Infof("Got signal [%d] to exit.", sig)
		svr.Shutdown(proxy.Reason{Reason: "signal", Message: fmt.Sprintf("%v", sig)})
	}()

	if cpuProfileFile != "" {
		f, err := os.Create(cpuProfileFile)
		if err != nil {
			u.Errorf("could not care cpu file: %v", err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	svr.Run()
}
