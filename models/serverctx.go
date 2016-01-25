package models

import (
	"fmt"
	"strings"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/schema"

	"github.com/dataux/dataux/planner"
	"github.com/dataux/dataux/planner/gridrunner"
)

type ServerCtx struct {
	Config  *Config
	schemas map[string]*schema.Schema
	RtConf  *datasource.RuntimeSchema
	Grid    *gridrunner.Server
}

func NewServerCtx(conf *Config) *ServerCtx {
	svr := ServerCtx{}
	svr.Config = conf
	svr.RtConf = datasource.NewRuntimeSchema()
	if conf.SupressRecover {
		svr.RtConf.DisableRecover = true
	}
	return &svr
}

func (m *ServerCtx) Init() error {

	if err := m.loadConfig(); err != nil {
		return err
	}
	m.Grid = planner.NewServerGrid(2)
	go m.Grid.RunMaster()
	return nil
}

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

		u.Debugf("parse schemas: %v", schemaConf)
		if _, ok := m.schemas[schemaConf.Name]; ok {
			panic(fmt.Sprintf("duplicate schema '%s'", schemaConf.Name))
		}

		sch := schema.NewSchema(schemaConf.Name)
		m.schemas[schemaConf.Name] = sch

		// find the Source config for eached named db/source
		for _, sourceName := range schemaConf.Sources {

			var sourceConf *schema.SourceConfig
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
			sourceConf.Init()

			sourceSchema := schema.NewSourceSchema(sourceName, sourceConf.SourceType)
			sourceSchema.Conf = sourceConf
			sourceSchema.Schema = sch
			//u.Infof("found sourceName: %q schema.Name=%q", sourceName, sourceSchema.Name)

			for _, nc := range m.Config.Nodes {
				if nc.Source == sourceConf.Name {
					sourceSchema.Nodes = append(sourceSchema.Nodes, nc)
					sourceConf.Nodes = append(sourceConf.Nodes, nc)
				}
			}

			//u.Warnf("source: %v has nodect: %v", sourceSchema.Name, len(sourceConf.Nodes))
			sourceFunc := DataSourceCreatorGet(sourceConf.SourceType)
			if sourceFunc == nil {
				//u.Warnf("Data source Not found for source_type: " + sourceConf.SourceType)
				//return fmt.Errorf("Could not find DataSource for %v", sourceConf.SourceType)
				continue
			}
			ds := sourceFunc(sourceSchema, m.Config)
			sourceSchema.DS = ds

			// TODO:   Periodically refresh this as sources are dynamic tables
			u.Infof("tables to load? %#v", sourceSchema.Conf)
			for _, tableName := range ds.Tables() {
				m.loadSourceSchema(strings.ToLower(tableName), sch, sourceSchema)
			}
			sch.SourceSchemas[sourceName] = sourceSchema
		}

	}

	return nil
}

func (m *ServerCtx) loadSourceSchema(tableName string, schema *schema.Schema, source *schema.SourceSchema) {
	tableLoad := true
	if len(source.Conf.TablesToLoad) > 0 {
		tableLoad = false
		for _, tbl := range source.Conf.TablesToLoad {
			if tbl == tableName {
				tableLoad = true
				break
			}
		}
	}
	if tableLoad {
		source.AddTableName(tableName)
	}
}

func (m *ServerCtx) Schema(db string) *schema.Schema {
	s := m.schemas[db]
	return s
}
