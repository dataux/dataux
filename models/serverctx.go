package models

import (
	"fmt"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/plan"
	"github.com/araddon/qlbridge/schema"

	"github.com/dataux/dataux/planner"
)

// Server Context for the DataUX Server giving access to the shared
//  memory objects Config, Schemas, Grid runtime
type ServerCtx struct {
	// The dataux server config info on schema, backends, frontends, etc
	Config *Config
	// The underlying qlbridge schema holds info about the
	//  available datasource Drivers/Adapters
	Reg *datasource.Registry
	// Grid is our real-time multi-node coordination and messaging system
	Grid *planner.Server

	schemas map[string]*schema.Schema
}

func NewServerCtx(conf *Config) *ServerCtx {
	svr := ServerCtx{}
	svr.Config = conf
	svr.Reg = datasource.DataSourcesRegistry()

	return &svr
}

// Load all the config info for this context and start the grid servers
func (m *ServerCtx) Init() error {

	if err := m.loadConfig(); err != nil {
		return err
	}

	planner.GridConf.NatsServers = m.Config.Nats
	planner.GridConf.EtcdServers = m.Config.Etcd

	// how many worker nodes?
	m.Grid = planner.NewServerGrid(2, m.Reg)

	return nil
}
func (m *ServerCtx) SchemaLoader(db string) (*schema.Schema, error) {
	s, ok := m.Reg.Schema(db)
	if s == nil || !ok {
		u.Warnf("Could not find schema for db=%s", db)
		return nil, schema.ErrNotFound
	}
	return s, nil
}

func (m *ServerCtx) JobMaker(ctx *plan.Context) (*planner.ExecutorGrid, error) {
	//ctx.Schema = testmysql.Schema
	u.Warnf("jobMaker, going to do a partial plan?")
	return planner.BuildExecutorUnPlanned(ctx, m.Grid)
}

// Get
func (m *ServerCtx) Table(schemaName, tableName string) (*schema.Table, error) {
	s, ok := m.schemas[schemaName]
	if ok {
		return s.Table(tableName)
	}
	return nil, fmt.Errorf("That schema %q not found", schemaName)
}

func (m *ServerCtx) loadConfig() error {

	m.schemas = make(map[string]*schema.Schema)

	for _, schemaConf := range m.Config.Schemas {

		//u.Debugf("parse schemas: %v", schemaConf)
		if _, ok := m.schemas[schemaConf.Name]; ok {
			panic(fmt.Sprintf("duplicate schema '%s'", schemaConf.Name))
		}

		sch := schema.NewSchema(schemaConf.Name)
		m.Reg.SchemaAdd(sch)

		// find the Source config for eached named db/source
		for _, sourceName := range schemaConf.Sources {

			var sourceConf *schema.ConfigSource
			// we must find a source conf by name
			for _, sc := range m.Config.Sources {
				//u.Debugf("sc: %s %#v", sourceName, sc)
				if sc.Name == sourceName {
					sourceConf = sc
					break
				}
			}
			if sourceConf == nil {
				u.Warnf("could not find source: %v", sourceName)
				return fmt.Errorf("Could not find Source Config for %v", sourceName)
			}

			ss := schema.NewSchemaSource(sourceName, sourceConf.SourceType)
			ss.Conf = sourceConf
			ss.Schema = sch
			//u.Infof("found sourceName: %q schema.Name=%q conf=%+v", sourceName, ss.Name, sourceConf)

			for _, nc := range m.Config.Nodes {
				if nc.Source == sourceConf.Name {
					ss.Nodes = append(ss.Nodes, nc)
					sourceConf.Nodes = append(sourceConf.Nodes, nc)
				}
			}

			sch.AddSourceSchema(ss)

			ds := m.Reg.Get(sourceConf.SourceType)
			//u.Debugf("after reg.Get(%q)  %#v", sourceConf.SourceType, ds)
			if ds == nil {
				//u.Debugf("could not find source for %v", sourceName)
			} else {
				ss.DS = ds
				ss.Partitions = sourceConf.Partitions
				if dsConfig, getsConfig := ss.DS.(schema.SourceSetup); getsConfig {
					if err := dsConfig.Setup(ss); err != nil {
						u.Errorf("Error setuping up %v  %v", sourceName, err)
					}
				}
				m.Reg.SourceSchemaAdd(ss)
			}

		}

	}

	return nil
}

func (m *ServerCtx) Schema(source string) (*schema.Schema, bool) {
	return m.Reg.Schema(source)
}
