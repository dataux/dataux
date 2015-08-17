package models

import (
	"fmt"
	"strings"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/datasource"
)

type ServerCtx struct {
	Config  *Config
	schemas map[string]*datasource.Schema
	RtConf  *datasource.RuntimeSchema
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
	return nil
}

func (m *ServerCtx) Table(schema, tableName string) (*datasource.Table, error) {
	s, ok := m.schemas[schema]
	if ok {
		return s.Table(tableName)
	}
	return nil, fmt.Errorf("That schema %q not found", schema)
}

func (m *ServerCtx) loadConfig() error {

	m.schemas = make(map[string]*datasource.Schema)

	for _, schemaConf := range m.Config.Schemas {

		//u.Debugf("parse schemas: %v", schemaConf)
		if _, ok := m.schemas[schemaConf.Name]; ok {
			panic(fmt.Sprintf("duplicate schema '%s'", schemaConf.Name))
		}

		schema := datasource.NewSchema(schemaConf.Name)
		m.schemas[schemaConf.Name] = schema

		// find the Source config for eached named db/source
		for _, sourceName := range schemaConf.Sources {

			var sourceConf *datasource.SourceConfig
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

			sourceSchema := datasource.NewSourceSchema(sourceName, sourceConf.SourceType)
			sourceSchema.Conf = sourceConf
			sourceSchema.Schema = schema
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
			sourceSchema.DSFeatures = datasource.NewFeaturedSource(ds)

			// TODO:   Periodically refresh this as sources are dynamic tables
			for _, tableName := range ds.Tables() {
				m.loadSourceSchema(strings.ToLower(tableName), schema, sourceSchema)
			}
			schema.SourceSchemas[sourceName] = sourceSchema
		}

	}

	return nil
}

func (m *ServerCtx) loadSourceSchema(tableName string, schema *datasource.Schema, source *datasource.SourceSchema) {
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

func (m *ServerCtx) Schema(db string) *datasource.Schema {
	s := m.schemas[db]
	return s
}
