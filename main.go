package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	// Backend Registrations Side-Effect imports
	_ "github.com/dataux/dataux/pkg/backends/elasticsearch"
	_ "github.com/dataux/dataux/pkg/backends/mongo"

	u "github.com/araddon/gou"
	"github.com/dataux/dataux/pkg/frontends"
	"github.com/dataux/dataux/pkg/models"
	"github.com/dataux/dataux/pkg/proxy"
	mysqlproxy "github.com/dataux/dataux/vendor/mixer/proxy"
)

var (
	configFile *string = flag.String("config", "dataux.conf", "dataux proxy config file")
	logLevel   *string = flag.String("loglevel", "debug", "log level [debug|info|warn|error]")
)

func main() {

	runtime.GOMAXPROCS(runtime.NumCPU())

	flag.Parse()

	if len(*configFile) == 0 {
		u.Errorf("must use a config file")
		return
	}
	u.SetupLogging(*logLevel)
	u.SetColorIfTerminal()

	// get config
	conf, err := models.LoadConfigFromFile(*configFile)
	if err != nil {
		u.Errorf("Could not load config: %v", err)
		os.Exit(1)
	}
	// Make Server Context
	svrCtx := models.NewServerCtx(conf)
	svrCtx.Init()

	// TODO:   these should be started by server through registry, imports
	mysqlHandler, err := frontends.NewMySqlHandler(svrCtx)
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

	svr.Run()
}
