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
	s := m.schemas[schema]
	if s != nil {
		return s.TableMap[tableName], nil
	}

	return nil, fmt.Errorf("Table not found %v", tableName)
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

			//u.Infof("found sourceConf: %#v", sourceConf)
			sourceSchema := datasource.NewSourceSchema(sourceName, sourceConf.SourceType)
			sourceSchema.Conf = sourceConf
			sourceSchema.Schema = schema

			for _, nc := range m.Config.Nodes {
				//u.Debugf("node: %#v", nc)
				if nc.Source == sourceConf.Name {
					//u.Infof("Found SourceType Match: %#v", nc)
					sourceSchema.Nodes = append(sourceSchema.Nodes, nc)
					//sourceSchema.address = nc.Address
				}
			}

			sourceFunc := DataSourceCreatorGet(sourceConf.SourceType)
			if sourceFunc == nil {
				//panic("Data source Not found for source_type: " + schemaConf.SourceType)
				u.Warnf("Data source Not found for source_type: " + sourceConf.SourceType)
				//return fmt.Errorf("Could not find DataSource for %v", sourceConf.SourceType)
				continue
			}
			ds := sourceFunc(sourceSchema, m.Config)
			sourceSchema.DS = ds
			sourceSchema.DSFeatures = datasource.NewFeaturedSource(ds)

			// TODO:   Periodically refresh this as es schema is dynamic
			for _, tableName := range ds.Tables() {
				tableName = strings.ToLower(tableName)
				if _, exists := schema.TableMap[tableName]; exists {
					return fmt.Errorf("Schemas may not contain more than one table of same name: %v", tableName)
				}
				m.loadSourceSchema(tableName, schema, sourceSchema)
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
		//u.Debugf("loading table %s", tableName)
		tbl, err := schema.Table(tableName)
		if err != nil {
			u.Errorf("Could not find table? %v", err)
		} else if tbl == nil {
			u.Errorf("Could not find table, nil: %v", tableName)
		} else {
			tbl.Schema = schema
			tbl.SourceSchema = source
			source.TableMap[tableName] = tbl
			schema.TableMap[tableName] = tbl
			schema.TableNames = append(schema.TableNames, tableName)
			source.TableNames = append(source.TableNames, tableName)
		}

	} else {
		//u.Debugf("not loading schema table: %v", tableName)
	}
}

func (m *ServerCtx) Schema(db string) *datasource.Schema {
	s := m.schemas[db]
	return s
}
