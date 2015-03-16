package models

import (
	"fmt"

	u "github.com/araddon/gou"
)

type ServerCtx struct {
	Config  *Config
	schemas map[string]*Schema
}

func NewServerCtx(conf *Config) *ServerCtx {
	svr := ServerCtx{}
	svr.Config = conf
	return &svr
}

func (m *ServerCtx) Init() error {
	if err := m.loadSchemasFromConfig(); err != nil {
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

func (m *ServerCtx) loadSchemasFromConfig() error {

	m.schemas = make(map[string]*Schema)

	for _, schemaConf := range m.Config.Schemas {

		u.Debugf("parse schemas: %v", schemaConf)
		if _, ok := m.schemas[schemaConf.DB]; ok {
			panic(fmt.Sprintf("duplicate schema '%s'", schemaConf.DB))
		}
		if len(schemaConf.Nodes) == 0 {
			u.Warnf("schema '%s' should have at least one node", schemaConf.DB)
		}

		schema := &Schema{
			Db:         schemaConf.DB,
			Address:    schemaConf.Address,
			SourceType: schemaConf.SourceType,
			Tables:     make(map[string]*Table),
			Conf:       schemaConf,
		}

		m.schemas[schemaConf.DB] = schema
		sourceFunc := DataSourceCreatorGet(schemaConf.SourceType)
		if sourceFunc == nil {
			//panic("Data source Not found for source_type: " + schemaConf.SourceType)
			u.Warnf("Data source Not found for source_type: " + schemaConf.SourceType)
		} else {
			schema.DataSource = sourceFunc(schema, m.Config)
			schema.DataSource.Init()
		}
	}

	return nil
}

func (m *ServerCtx) Schema(db string) *Schema {
	u.Debugf("get schema for %s", db)
	return m.schemas[db]
}
