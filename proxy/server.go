package proxy

import (
	u "github.com/araddon/gou"
	"github.com/dataux/dataux/models"

	"strings"
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

// Server is the main DataUx server, the running process and responsible for:
//  1) starting *listeners* - network transports/protocols (mysql,mongo,redis)
//  2) routing requests through *Handlers*(plugins) which
//      filter, transform, log, etc
//  3) managing backend-transport connections
//  4) executing statements amongst backends with results
type Server struct {
	conf *models.Config
	ctx  *models.ServerCtx

	// Frontend listener is a Listener Protocol handler
	// to listen on specific port such as mysql
	frontends []models.Listener

	// any handlers/transforms etc
	handlers []models.ConnectionHandle

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

	if len(m.frontends) == 0 {
		u.Errorf("No frontends: ")
		return
	}
	for i, frontend := range m.frontends {
		u.Debugf("starting frontend: %T", frontend)
		go func() {
			defer func() {
				if r := recover(); r != nil {
					u.Errorf("frontend shutdown: %v", r)
				}
			}()
			// Blocking runner
			if err := frontend.Run(m.handlers[i], m.stop); err != nil {
				u.Errorf("error on frontend? %#v %v", frontend, err)
				m.Shutdown(Reason{"error", err, ""})
			}
		}()
	}

	// block forever
	<-m.stop
	// after shutdown, ensure they are all closed
	for _, frontend := range m.frontends {
		if err := frontend.Close(); err != nil {
			u.Errorf("Error shuting down %v", err)
		}
	}
}

func (m *Server) loadFrontends() error {

	for name, frontendSetup := range models.Listeners() {
		u.Debugf("looking for frontend for %v", name)
		for _, feConf := range m.conf.Frontends {
			u.Debugf("frontendconf:  %#v", feConf)
			if feConf.Type == name {
				frontend, err := frontendSetup.ListenerInit(feConf, m.conf)
				if err != nil {
					u.Errorf("Could not get frontend", err)
					return err
				}
				m.handlers = append(m.handlers, frontendSetup.ConnectionHandle)
				m.frontends = append(m.frontends, frontend)
				u.Infof("Loaded frontend %s ", name)
			}
		}
	}

	for _, feConf := range m.conf.Frontends {
		u.Debugf("fe %#v", feConf)
	}
	return nil
}

// Shutdown listeners and close down
func (m *Server) Shutdown(reason Reason) {
	m.stop <- true
}
