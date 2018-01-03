package models

import (
	"database/sql/driver"
	"fmt"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/datasource/memdb"
	"github.com/araddon/qlbridge/plan"
	"github.com/araddon/qlbridge/schema"

	"github.com/dataux/dataux/planner"
)

// ServerCtx Singleton global Context for the DataUX Server giving
// access to the shared Config, Schemas, Grid runtime
type ServerCtx struct {
	// The dataux server config info on schema, backends, frontends, etc
	Config *Config
	// The underlying qlbridge registry holds info about the available datasource providers
	Reg *schema.Registry
	// PlanGrid is swapping out the qlbridge planner
	// with a distributed version that uses Grid lib to split
	// tasks across nodes
	PlanGrid       *planner.PlannerGrid
	schemas        map[string]*schema.Schema
	internalSchema *schema.Schema
}

// NewServerCtx create new server context.   Main stateful object
// for sharing server state.
func NewServerCtx(conf *Config) *ServerCtx {
	svr := ServerCtx{}
	svr.Config = conf
	svr.Reg = schema.DefaultRegistry()
	return &svr
}

// Init Load all the config info for this server and start the
// grid/messaging/coordination systems
func (m *ServerCtx) Init() error {

	m.loadInternalSchema()

	if err := m.loadConfig(); err != nil {
		return err
	}

	m.Reg.Init()

	for _, s := range m.Reg.Schemas() {
		if _, exists := m.schemas[s]; !exists {
			// new from init
			sch, _ := m.Reg.Schema(s)
			if sch != nil {
				m.schemas[s] = sch
			}
		}
	}

	// Copy over the nats, etcd info from config to
	// Planner grid
	planner.GridConf.EtcdServers = m.Config.Etcd

	// how many worker nodes?
	if m.Config.WorkerCt == 0 {
		m.Config.WorkerCt = 2
	}

	m.PlanGrid = planner.NewPlannerGrid(m.Config.WorkerCt, m.Reg)

	return nil
}

// SchemaLoader finds a schema by name from the registry
func (m *ServerCtx) SchemaLoader(db string) (*schema.Schema, error) {
	s, ok := m.Reg.Schema(db)
	if s == nil || !ok {
		u.Warnf("Could not find schema for db=%s", db)
		return nil, schema.ErrNotFound
	}
	return s, nil
}

// InfoSchema Get A schema
func (m *ServerCtx) InfoSchema() (*schema.Schema, error) {
	if len(m.schemas) == 0 {
		for _, sc := range m.Config.Schemas {
			s, ok := m.Reg.Schema(sc.Name)
			if s != nil && ok {
				u.Warnf("%p found schema for db=%q", m, sc.Name)
				return s, nil
			}
		}
		return nil, schema.ErrNotFound
	}
	for _, s := range m.schemas {
		return s, nil
	}
	panic("unreachable")
}

// JobMaker create job
func (m *ServerCtx) JobMaker(ctx *plan.Context) (*planner.GridTask, error) {
	//u.Debugf("jobMaker, going to do a partial plan?")
	return planner.BuildExecutorUnPlanned(ctx, m.PlanGrid)
}

// Table Get by schema, name
func (m *ServerCtx) Table(schemaName, tableName string) (*schema.Table, error) {
	s, ok := m.schemas[schemaName]
	if ok {
		return s.Table(tableName)
	}
	return nil, fmt.Errorf("That schema %q not found", schemaName)
}

func (m *ServerCtx) loadInternalSchema() {
	inrow := []driver.Value{1, "not implemented"}
	cols := []string{"id", "worker"}
	db, err := memdb.NewMemDbData("workers", [][]driver.Value{inrow}, cols)
	if err != nil {
		u.Errorf("could not create worker list store %v", err)
	}
	schema.RegisterSourceAsSchema("server_schema", db)
	var ok bool
	m.internalSchema, ok = m.Reg.Schema("server_schema")
	if !ok {
		u.Errorf("could not create internal schema")
	}
	m.Config.Sources = append(m.Config.Sources, &schema.ConfigSource{SourceType: "server_schema", Name: "server_schema"})
	m.Config.Schemas = append(m.Config.Schemas, &schema.ConfigSchema{Name: "server_schema"})
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

			childSchema := schema.NewSchema(sourceName)
			// we must find a source conf by name
			for _, sc := range m.Config.Sources {
				if sc.Name == sourceName {
					childSchema.Conf = sc
					break
				}
			}
			if childSchema.Conf == nil {
				u.Warnf("could not find source: %v", sourceName)
				return fmt.Errorf("Could not find Source Config for %v", sourceName)
			}

			//u.Infof("found sourceName: %q schema.Name=%q conf=%+v", sourceName, childSchema.Name, childSchema.Conf)

			sourceConf := childSchema.Conf

			ds, err := m.Reg.GetSource(sourceConf.SourceType)
			if err != nil {
				u.Warnf("could not get source %v err=%v", sourceConf.SourceType, err)
				return err
			}
			if ds == nil {
				u.Warnf("could not find source for %v  %v", sourceName, sourceConf.SourceType)
			} else {
				childSchema.DS = ds
				if err := childSchema.DS.Setup(childSchema); err != nil {
					u.Errorf("Error setting up %v  %v", sourceName, err)
				}
			}
			m.Reg.SchemaAddChild(schemaConf.Name, childSchema)
		}
	}

	return nil
}

func (m *ServerCtx) Schema(source string) (*schema.Schema, bool) {
	return m.Reg.Schema(source)
}
