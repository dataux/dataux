package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	u "github.com/araddon/gou"
	"github.com/dataux/dataux/pkg/backends/elasticsearch"
	"github.com/dataux/dataux/pkg/models"
	"github.com/dataux/dataux/pkg/proxy"
	mysqlproxy "github.com/dataux/dataux/vendor/mixer/proxy"
)

var (
	configFile *string = flag.String("config", "dataux.conf", "dataux proxy config file")
	logLevel   *string = flag.String("loglevel", "debug", "log level [debug|info|warn|error]")
	backend    *string = flag.String("handler", "elasticsearch", "handler/backend [mysql,elasticsearch] ")
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

	var handler models.Handler
	switch *backend {
	case "mysql":
		handler, err = mysqlproxy.NewHandlerSharded(conf)
		if err != nil {
			u.Errorf("Could not create handlers: %v", err)
			os.Exit(1)
		}
	case "elasticsearch":
		handler, err = elasticsearch.NewMySqlHandler(conf)
		if err != nil {
			u.Errorf("Could not create handlers: %v", err)
			os.Exit(1)
		}
	default:
		u.Errorf("Invalid handler: %v", *backend)
		os.Exit(1)
	}

	// Load our Frontend Listener's
	models.ListenerRegister(mysqlproxy.ListenerType,
		mysqlproxy.ListenerInit,
		handler,
	)

	var svr *proxy.Server
	svr, err = proxy.NewServer(conf)
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
