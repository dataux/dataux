package models

import (
	"fmt"
	"strings"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/datasource"
)

type ServerCtx struct {
	Config  *Config
	schemas map[string]*Schema
	RtConf  *datasource.RuntimeConfig
}

func NewServerCtx(conf *Config) *ServerCtx {
	svr := ServerCtx{}
	svr.Config = conf
	svr.RtConf = datasource.NewRuntimeConfig()
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

func (m *ServerCtx) Table(schema, tableName string) (*Table, error) {
	s := m.schemas[schema]
	if s != nil {
		return s.Tables[tableName], nil
	}

	return nil, fmt.Errorf("Table not found %v", tableName)
}

func (m *ServerCtx) loadConfig() error {

	m.schemas = make(map[string]*Schema)

	for _, schemaConf := range m.Config.Schemas {

		u.Debugf("parse schemas: %v", schemaConf)
		if _, ok := m.schemas[schemaConf.Name]; ok {
			panic(fmt.Sprintf("duplicate schema '%s'", schemaConf.Name))
		}

		schema := &Schema{
			Name:          schemaConf.Name,
			Tables:        make(map[string]*Table),
			SourceSchemas: make(map[string]*SourceSchema),
			TableNames:    make([]string, 0),
			Conf:          schemaConf,
		}

		m.schemas[schemaConf.Name] = schema

		// find the Source config for eached named db/source
		for _, sourceName := range schemaConf.Sources {
			var sourceConf *SourceConfig
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

			//u.Infof("found sourceConf: %#v", sourceConf)
			sourceSchema := &SourceSchema{
				Db:         sourceConf.Name,
				Tables:     make(map[string]*Table),
				TableNames: make([]string, 0),
				Conf:       sourceConf,
				Schema:     schema,
				Nodes:      make([]*NodeConfig, 0),
			}

			for _, nc := range m.Config.Nodes {
				//u.Debugf("node: %#v", nc)
				if nc.SourceType == sourceConf.SourceType {
					//u.Infof("Found SourceType Match: %#v", nc)
					sourceSchema.Nodes = append(sourceSchema.Nodes, nc)
					sourceSchema.address = nc.Address
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
			sourceSchema.DataSource = datasource.NewFeaturedSource(ds)

			for _, tableName := range ds.Tables() {
				tableName = strings.ToLower(tableName)
				if _, exists := schema.Tables[tableName]; exists {
					return fmt.Errorf("Schemas may not contain more than one table of same name: %v", tableName)
				}
				tbl, err := ds.Table(tableName)
				if err != nil {
					u.Errorf("Could not find table? %v", err)
					return err
				}

				tbl.Schema = schema
				tbl.SourceSchema = sourceSchema
				sourceSchema.Tables[tableName] = tbl
				schema.Tables[tableName] = tbl
				schema.TableNames = append(schema.TableNames, tableName)
				sourceSchema.TableNames = append(sourceSchema.TableNames, tableName)
				//u.Debugf("found table: %#v", tbl)
			}
			//u.Infof("found datasource: source=%v  %T %#v", sourceName, ds, ds)
			//u.Debugf("tables? %v", schema.TableNames)

			schema.SourceSchemas[sourceName] = sourceSchema

		}

	}

	return nil
}

func (m *ServerCtx) Schema(db string) *Schema {
	s := m.schemas[db]
	//u.Debugf("get schema for %s   %#v", db, s)
	return s
}
