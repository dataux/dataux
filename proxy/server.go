package proxy

import (
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	u "github.com/araddon/gou"
	"github.com/dataux/dataux/models"
)

var asciiIntro = `
     _       _
    | |     | |
  __| | __ _| |_ __ _ _   ___  __
 / _* |/ _* | __/ _* | | | \ \/ /
| (_| | (_| | || (_| | |_| |>  <
 \__,_|\__,_|\__\__,_|\__,_/_/\_\

`

func banner() string {
	return strings.Replace(asciiIntro, "*", "`", -1)
}

func RunDaemon(configFile string) {
	// get config
	conf, err := models.LoadConfigFromFile(configFile)
	if err != nil {
		u.Errorf("Could not load config: %v", err)
		os.Exit(1)
	}
	// Make Server Context
	svrCtx := models.NewServerCtx(conf)
	svrCtx.Init()

	var svr *Server
	svr, err = NewServer(svrCtx)
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
		svr.Shutdown(Reason{Reason: "signal", Message: fmt.Sprintf("%v", sig)})
	}()

	svr.Run()
}

// Server is the main DataUx server, the running process and responsible for:
//  1) starting *listeners* - network transports/protocols (mysql,mongo,redis)
//  2) routing requests through *Handlers*(plugins) which
//      filter, transform, log, etc
//  3) connecting front-end requests to the execution/planning engine
type Server struct {
	conf *models.Config
	ctx  *models.ServerCtx

	// Frontend listener is a Listener Protocol handler
	// to listen on specific port such as mysql
	listeners []models.Listener

	stop chan bool
}

type Reason struct {
	Reason  string
	err     error
	Message string
}

func NewServer(ctx *models.ServerCtx) (*Server, error) {

	svr := &Server{conf: ctx.Config, ctx: ctx, stop: make(chan bool)}

	if err := svr.loadFrontends(); err != nil {
		return nil, err
	}

	return svr, nil
}

// Run is a blocking runner, that starts listeners
// and returns if connection to listeners cannot be established
func (m *Server) Run() {

	if len(m.listeners) == 0 {
		u.Errorf("No frontends found ")
		return
	}

	for _, listener := range m.listeners {
		u.Debugf("starting listener: %T", listener)
		go func(l models.Listener) {
			defer func() {
				if r := recover(); r != nil {
					u.Errorf("listener shutdown: %v", r)
				}
			}()
			// Blocking runner
			if err := l.Run(m.stop); err != nil {
				u.Errorf("error on frontend? %#v %v", l, err)
				m.Shutdown(Reason{"error", err, ""})
			}
		}(listener)
	}

	// block until shutdown signal
	u.Debug("\n", banner(), "\n")
	<-m.stop

	// after shutdown, ensure they are all closed
	for _, listener := range m.listeners {
		if err := listener.Close(); err != nil {
			u.Errorf("Error shuting down %T err=%v", listener, err)
		}
	}
}

func (m *Server) loadFrontends() error {

	for name, listener := range models.Listeners() {
		//u.Debugf("looking for frontend for %v", name)
		for _, listenConf := range m.conf.Frontends {

			if listenConf.Type == name {
				u.Debugf("found listener conf:  %#v", listenConf)
				err := listener.Init(listenConf, m.ctx)
				if err != nil {
					u.Errorf("Could not get frontend", err)
					return err
				}
				m.listeners = append(m.listeners, listener)
				//u.Infof("Loaded listener %s ", name)
			}
		}
	}
	return nil
}

// Shutdown listeners and close down
func (m *Server) Shutdown(reason Reason) {
	m.stop <- true
}
