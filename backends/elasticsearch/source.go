package elasticsearch

import (
	"encoding/json"
	"fmt"
	"strings"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/schema"
	"github.com/araddon/qlbridge/value"
)

var (
	// ensure Elasticsearch implement Source interfaces
	_ schema.Source = (*Source)(nil)
)

const (
	// SourceType defines the "source" type from qlbridge datasource
	SourceType = "elasticsearch"
)

func init() {
	// We need to register our DataSource provider here
	schema.RegisterSourceType(SourceType, &Source{})
}

// Source is the elasticsearch datasource
type Source struct {
	schema   *schema.Schema
	conf     *schema.ConfigSource
	tables   []string // lower cased
	tablemap map[string]*schema.Table
}

// Init this source
func (m *Source) Init() {}

// Setup this source
func (m *Source) Setup(ss *schema.Schema) error {

	if m.schema != nil {
		return nil
	}

	m.schema = ss
	m.conf = ss.Conf
	m.tablemap = make(map[string]*schema.Table)

	if ss.Conf != nil && len(ss.Conf.Partitions) > 0 {
		// not yet implemented.  Do we need to on es?  probably
		// not as we don't plan on poly-filling.
	}

	u.Debugf("Init() Eleasticsearch schema P=%p", m.schema)
	if err := m.loadTableNames(); err != nil {
		u.Errorf("could not load es tables: %v", err)
		return err
	}
	if m.schema != nil {
		u.Debugf("Post Init() Eleasticsearch schema P=%p tblct=%d", m.schema, len(m.schema.Tables()))
	}
	return nil
}

// Open open connection to elasticsearch source.
func (m *Source) Open(schemaName string) (schema.Conn, error) {
	//u.Debugf("Open(%v)", schemaName)
	tbl, err := m.schema.Table(schemaName)
	if err != nil {
		return nil, err
	}
	if tbl == nil {
		u.Errorf("Could not find table for '%s'.'%s'", m.schema.Name, schemaName)
		return nil, fmt.Errorf("Could not find '%v'.'%v' schema", m.schema.Name, schemaName)
	}

	sqlConverter := NewSqlToEs(tbl)
	return sqlConverter, nil
}

// Close this source.
func (m *Source) Close() error { return nil }

//func (m *Source) DataSource() schema.Source { return m }

// Tables list of tablenames
func (m *Source) Tables() []string { return m.tables }

// Table get a single table.
func (m *Source) Table(table string) (*schema.Table, error) {
	u.Debugf("get table for %s", table)
	t := m.tablemap[table]
	if t != nil {
		return t, nil
	}
	tlower := strings.ToLower(table)
	t = m.tablemap[tlower]
	if t != nil {
		return t, nil
	}
	return m.loadTableSchema(table)
}

// Load only table names, not full schema
func (m *Source) loadTableNames() error {

	host := chooseBackend(m.schema)
	if host == "" {
		u.Errorf("missing address: %#v", m.schema)
		return fmt.Errorf("Could not find Elasticsearch Host Address: %v", host)
	}

	jh, err := u.JsonHelperHttp("GET", fmt.Sprintf("%s/_aliases", host), nil)
	if err != nil {
		u.Error("error on es read: %v", err)
		return err
	}

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
	//u.Debugf("resp: %v", jh)
	if len(m.schema.Conf.TablesToLoad) > 0 {
		tableMap := make(map[string]struct{}, len(m.schema.Conf.TablesToLoad))
		for _, tableToLoad := range m.schema.Conf.TablesToLoad {
			tableMap[tableToLoad] = struct{}{}
		}
		temp := tables
		tables = []string{}
		for _, t := range temp {
			if _, loadTable := tableMap[t]; loadTable {
				tables = append(tables, t)
			}
		}
	}

	m.tables = tables
	u.Debugf("found tables: %v", m.tables)

	return nil
}

func (m *Source) loadTableSchema(table string) (*schema.Table, error) {

	if m.schema == nil {
		return nil, fmt.Errorf("no schema in use")
	}

	host := chooseBackend(m.schema)
	if host == "" {
		u.Errorf("missing address: %#v", m.schema)
		return nil, fmt.Errorf("Could not find Elasticsearch Host Address: %v", table)
	}
	tbl := schema.NewTable(table)

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
		return nil, fmt.Errorf("Could not load elasticsearch table %q", table)
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

	buildEsFields(m.schema, tbl, jh, "", 0)

	keys := make([]string, len(tbl.Fields))
	for i, f := range tbl.Fields {
		keys[i] = f.Name
	}
	tbl.SetColumns(keys)
	m.tablemap[tbl.Name] = tbl

	return tbl, nil
}

func buildEsFields(s *schema.Schema, tbl *schema.Table, jh u.JsonHelper, prefix string, depth int) {
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
