package elasticsearch

import (
	"encoding/json"
	"fmt"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/schema"
	"github.com/araddon/qlbridge/value"
)

var (
	// implement interfaces
	_ schema.DataSource = (*ElasticsearchDataSource)(nil)
)

const (
	ListenerType = "elasticsearch"
)

func init() {
	// We need to register our DataSource provider here
	datasource.Register(ListenerType, &ElasticsearchDataSource{})
}

type ElasticsearchDataSource struct {
	srcschema *schema.SourceSchema
	conf      *schema.SourceConfig
}

func (m *ElasticsearchDataSource) Setup(ss *schema.SourceSchema) error {

	if m.srcschema != nil {
		return nil
	}

	m.srcschema = ss
	m.conf = ss.Conf

	if ss.Conf != nil && len(ss.Conf.Partitions) > 0 {

	}

	u.Debugf("Init() Eleasticsearch schema P=%p", m.srcschema)
	if err := m.findEsNodes(); err != nil {
		u.Errorf("could not init es: %v", err)
		return err
	}

	if err := m.loadTableNames(); err != nil {
		u.Errorf("could not load es tables: %v", err)
		return err
	}
	if m.srcschema != nil {
		u.Debugf("Post Init() Eleasticsearch schema P=%p tblct=%d", m.srcschema, len(m.srcschema.Tables()))
	}
	return nil
}

func (m *ElasticsearchDataSource) Open(schemaName string) (schema.SourceConn, error) {
	//u.Debugf("Open(%v)", schemaName)
	tbl, err := m.srcschema.Table(schemaName)
	if err != nil {
		return nil, err
	}
	if tbl == nil {
		u.Errorf("Could not find table for '%s'.'%s'", m.srcschema.Name, schemaName)
		return nil, fmt.Errorf("Could not find '%v'.'%v' schema", m.srcschema.Name, schemaName)
	}

	sqlConverter := NewSqlToEs(tbl)
	return sqlConverter, nil
}

func (m *ElasticsearchDataSource) Close() error { return nil }

func (m *ElasticsearchDataSource) DataSource() schema.DataSource {
	return m
}

func (m *ElasticsearchDataSource) Tables() []string {
	return m.srcschema.Tables()
}

func (m *ElasticsearchDataSource) Table(table string) (*schema.Table, error) {
	u.Debugf("get table for %s", table)
	return m.loadTableSchema(table)
}

// Load only table names, not full schema
func (m *ElasticsearchDataSource) loadTableNames() error {

	host := chooseBackend(m.srcschema)
	if host == "" {
		u.Errorf("missing address: %#v", m.srcschema)
		return fmt.Errorf("Could not find Elasticsearch Host Address: %v", host)
	}

	jh, err := u.JsonHelperHttp("GET", fmt.Sprintf("%s/_aliases", host), nil)
	if err != nil {
		u.Error("error on es read: %v", err)
		return err
	}
	//u.Debugf("resp: %v", jh)
	tables := []string{}
	for alias, _ := range jh {
		//u.Debugf("alias: %s", alias)
		if aliasJh := jh.Helper(alias + ".aliases"); len(aliasJh) > 0 {
			//u.Infof("has aliases? %#v", aliasJh)
			for aliasName, _ := range aliasJh {
				tables = append(tables, aliasName)
			}
		} else {
			tables = append(tables, alias)
		}
	}
	if len(m.srcschema.Conf.TablesToLoad) > 0 {
		tableMap := make(map[string]struct{}, len(m.srcschema.Conf.TablesToLoad))
		for _, tableToLoad := range m.srcschema.Conf.TablesToLoad {
			tableMap[tableToLoad] = struct{}{}
		}
		for _, table := range tables {
			if _, ok := tableMap[table]; ok {
				m.srcschema.AddTableName(table)
			}
		}
	} else {
		for _, table := range tables {
			m.srcschema.AddTableName(table)
		}
	}

	u.Debugf("found tables: %v", m.srcschema.Tables())

	return nil
}

func (m *ElasticsearchDataSource) loadTableSchema(table string) (*schema.Table, error) {

	if m.srcschema == nil {
		return nil, fmt.Errorf("no schema in use")
	}

	host := chooseBackend(m.srcschema)
	if host == "" {
		u.Errorf("missing address: %#v", m.srcschema)
		return nil, fmt.Errorf("Could not find Elasticsearch Host Address: %v", table)
	}
	tbl := schema.NewTable(table, m.srcschema)

	indexUrl := fmt.Sprintf("%s/%s/_mapping", host, tbl.Name)
	respJh, err := u.JsonHelperHttp("GET", indexUrl, nil)
	if err != nil {
		u.Error("error on es read: url=%v  err=%v", indexUrl, err)
	}
	//u.Debugf("url: %v", indexUrl)
	dataJh := respJh.Helper(table).Helper("mappings")
	if len(dataJh) == 0 {
		// This is an aliased index
		nonAliasTable := ""
		for nonAliasTable, _ = range respJh {
			break
		}
		u.Debugf("found non aliased table: %v", nonAliasTable)
		dataJh = respJh.Helper(nonAliasTable).Helper("mappings")
	}
	respKeys := dataJh.Keys()
	//u.Infof("keys:%v  resp:%v", respKeys, respJh)
	if len(respKeys) < 1 {
		u.Errorf("could not get data? %v   %v", indexUrl, respJh)
		u.LogTracef(u.WARN, "wat?")
		return nil, fmt.Errorf("Could not process desribe")
	}
	indexType := "user"
	for _, key := range respKeys {
		if key != "_default_" {
			indexType = key
			break
		}
	}

	jh := dataJh.Helper(indexType)
	//u.Debugf("resp: %v", jh)
	jh = jh.Helper("properties")

	tbl.AddField(schema.NewField("_id", value.StringType, 24, schema.NoNulls, nil, "PRI", "", "AUTOGEN"))
	tbl.AddField(schema.NewFieldBase("type", value.StringType, 24, "tbd"))
	tbl.AddField(schema.NewFieldBase("_score", value.NumberType, 24, "Created per Search By Elasticsearch"))

	buildEsFields(m.srcschema, tbl, jh, "", 0)

	m.srcschema.AddTable(tbl)

	return tbl, nil
}

func buildEsFields(s *schema.SourceSchema, tbl *schema.Table, jh u.JsonHelper, prefix string, depth int) {
	for field, _ := range jh {

		if h := jh.Helper(field); len(h) > 0 {
			jb, _ := json.Marshal(h)
			//jb, _ := json.MarshalIndent(h, " ", " ")
			fieldName := prefix + field
			var fld *schema.Field
			//u.Infof("%v %v", fieldName, h)
			switch esType := h.String("type"); esType {
			case "boolean":
				fld = schema.NewFieldBase(fieldName, value.BoolType, 1, string(jb))
			case "string":
				fld = schema.NewFieldBase(fieldName, value.StringType, 512, string(jb))
			case "date":
				fld = schema.NewFieldBase(fieldName, value.TimeType, 32, string(jb))
			case "int", "long", "integer":
				fld = schema.NewFieldBase(fieldName, value.IntType, 46, string(jb))
			case "double", "float":
				fld = schema.NewFieldBase(fieldName, value.NumberType, 64, string(jb))
			case "nested", "object":
				fld = schema.NewFieldBase(fieldName, value.StringType, 2000, string(jb))
			default:
				fld = schema.NewFieldBase(fieldName, value.StringType, 2000, `{"type":"object"}`)
				props := h.Helper("properties")
				if len(props) > 0 {
					buildEsFields(s, tbl, props, fieldName+".", depth+1)
				} else {
					u.Debugf("unknown type: '%v'  '%v'", esType, string(jb))
				}

			}
			if fld != nil {
				tbl.AddField(fld)
			}

		}
	}
}

func (m *ElasticsearchDataSource) findEsNodes() error {

	return nil
}
