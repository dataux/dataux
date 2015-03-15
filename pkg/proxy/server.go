package proxy

import (
	u "github.com/araddon/gou"
	"github.com/dataux/dataux/pkg/models"

	"fmt"
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

	// Frontend listener is a Listener Protocol handler
	// to listen on specific port such as mysql
	frontends []models.Listener

	// any handlers/transforms etc
	handlers []models.Handler

	// backends
	backends map[string]*models.BackendConfig

	// schemas
	schemas map[string]*models.Schema

	stop chan bool
}

type Reason struct {
	Reason  string
	err     error
	Message string
}

func NewServer(conf *models.Config) (*Server, error) {

	svr := &Server{conf: conf, stop: make(chan bool)}

	svr.backends = make(map[string]*models.BackendConfig)

	if err := svr.setupBackends(); err != nil {
		return nil, err
	}

	if err := setupSchemas(svr); err != nil {
		u.Errorf("schema: %v", err)
		return nil, err
	}

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
				m.handlers = append(m.handlers, frontendSetup.Handler)
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

//Find and setup/validate backend nodes
func (m *Server) setupBackends() error {

	for _, beConf := range m.conf.Backends {
		err := m.AddBackend(beConf)
		if err != nil {
			return err
		}
	}

	return nil
}

// Given a backend server config, add it to server/proxy
//  find the backend runner (starts network for backend)
//
func (m *Server) AddBackend(beConf *models.BackendConfig) error {

	beConf.Name = strings.ToLower(beConf.Name)

	if _, ok := m.backends[beConf.Name]; ok {
		return fmt.Errorf("duplicate backend [%s].", beConf.Name)
	}

	if beConf.BackendType == "" {
		for _, schemaConf := range m.conf.Schemas {
			for _, schemaBe := range schemaConf.Backends {
				if schemaBe == beConf.Name {
					beConf.BackendType = schemaConf.BackendType
				}
			}
		}
		if beConf.BackendType == "" {
			u.Warnf("no backendtype found from schemas for %s", beConf.Name)
		}
	}

	m.backends[beConf.Name] = beConf

	return nil
}

func (m *Server) BackendFind(serverName string) *models.BackendConfig {
	return m.backends[serverName]
}

func setupSchemas(s *Server) error {

	s.schemas = make(map[string]*models.Schema)

	for _, schemaConf := range s.conf.Schemas {
		if schemaConf.BackendType == "" {
			return fmt.Errorf("Must have backend_type %s", schemaConf)
		}
		if _, ok := s.schemas[schemaConf.DB]; ok {
			return fmt.Errorf("duplicate schema `%s`", schemaConf.DB)
		}
		if len(schemaConf.Backends) == 0 {
			u.Warnf("schema '%s' should have a node?", schemaConf.DB)
			//return fmt.Errorf("schema '%s' must have a node", schemaConf.DB)
		}

		nodes := make(map[string]*models.BackendConfig)
		for _, serverName := range schemaConf.Backends {

			be := s.BackendFind(serverName)
			if be == nil {
				return fmt.Errorf("schema '%s' node '%s' config is not exists.", schemaConf.DB, serverName)
			}

			if _, ok := nodes[serverName]; ok {
				return fmt.Errorf("schema '%s' node '%s' duplicate.", schemaConf.DB, serverName)
			}

			nodes[serverName] = be
		}

		s.schemas[schemaConf.DB] = &models.Schema{
			Db:    schemaConf.DB,
			Nodes: nodes,
		}
		u.Debugf("found schema:  %v", schemaConf.String())
		// rule:  rule,
	}

	return nil
}

func (s *Server) getSchema(db string) *models.Schema {
	return s.schemas[db]
}
