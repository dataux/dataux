package elasticsearch

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"strings"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
	"github.com/dataux/dataux/pkg/models"
)

var (
	// implement interfaces
	_ datasource.DataSource = (*ElasticsearchDataSource)(nil)
)

const (
	ListenerType = "elasticsearch"
)

func init() {
	// We need to register our DataSource provider here
	models.DataSourceRegister("elasticsearch", NewElasticsearchDataSource)
}

type ElasticsearchDataSource struct {
	schema *datasource.SourceSchema
	conf   *models.Config
}

func NewElasticsearchDataSource(schema *datasource.SourceSchema, conf *models.Config) models.DataSource {
	es := ElasticsearchDataSource{}
	es.schema = schema
	es.conf = conf
	// Register our datasource.Datasources in registry
	es.Init()
	datasource.Register(ListenerType, &es)
	return &es
}

func (m *ElasticsearchDataSource) Init() error {

	u.Debugf("Init() Eleasticsearch schema P=%p", m.schema)
	if err := m.findEsNodes(); err != nil {
		u.Errorf("could not init es: %v", err)
		return err
	}

	if err := m.loadTableNames(); err != nil {
		u.Errorf("could not load es tables: %v", err)
		return err
	}
	if m.schema != nil {
		u.Debugf("Post Init() Eleasticsearch schema P=%p tblct=%d", m.schema, len(m.schema.Tables()))
	}
	return nil
}

func (m *ElasticsearchDataSource) Builder(stmt *expr.SqlSelect) (expr.SubVisitor, error) {

	u.Debugf("get sourceTask for %v", stmt)
	tblName := strings.ToLower(stmt.From[0].Name)

	tbl, err := m.schema.Table(tblName)
	if err != nil {
		return nil, err
	}
	if tbl == nil {
		u.Errorf("Could not find table for '%s'.'%s'", m.schema.Name, tblName)
		return nil, fmt.Errorf("Could not find '%v'.'%v' schema", m.schema.Name, tblName)
	}

	es := NewSqlToEs(tbl)
	u.Debugf("sqltoes: %#v", es)
	return es, nil
}

/*
func (m *ElasticsearchDataSource) SourceTask(stmt *expr.SqlSelect) (models.SourceTask, error) {

	u.Debugf("get sourceTask for %v", stmt)
	tblName := strings.ToLower(stmt.From[0].Name)

	tbl, err := m.schema.Table(tblName)
	if err != nil {
		return nil, err
	}
	if tbl == nil {
		u.Errorf("Could not find table for '%s'.'%s'", m.schema.Name, tblName)
		return nil, fmt.Errorf("Could not find '%v'.'%v' schema", m.schema.Name, tblName)
	}

	es := NewSqlToEs(tbl)
	u.Debugf("sqltoes: %#v", es)
	resp, err := es.Query(stmt)
	if err != nil {
		u.Error(err)
		return nil, err
	}

	return resp, nil
}
*/
func (m *ElasticsearchDataSource) Open(schemaName string) (datasource.SourceConn, error) {
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

func (m *ElasticsearchDataSource) Close() error { return nil }

func (m *ElasticsearchDataSource) DataSource() datasource.DataSource {
	return m
}

func (m *ElasticsearchDataSource) Tables() []string {
	return m.schema.Tables()
}

func (m *ElasticsearchDataSource) Table(table string) (*datasource.Table, error) {
	u.Debugf("get table for %s", table)
	return m.loadTableSchema(table)
}

// Load only table names, not full schema
func (m *ElasticsearchDataSource) loadTableNames() error {

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
	if len(m.schema.Conf.TablesToLoad) > 0 {
		tableMap := make(map[string]struct{}, len(m.schema.Conf.TablesToLoad))
		for _, tableToLoad := range m.schema.Conf.TablesToLoad {
			tableMap[tableToLoad] = struct{}{}
		}
		for _, table := range tables {
			if _, ok := tableMap[table]; ok {
				m.schema.AddTableName(table)
			}
		}
	} else {
		for _, table := range tables {
			m.schema.AddTableName(table)
		}
	}

	u.Debugf("found tables: %v", m.schema.Tables())

	return nil
}

func (m *ElasticsearchDataSource) loadTableSchema(table string) (*datasource.Table, error) {

	if m.schema == nil {
		return nil, fmt.Errorf("no schema in use")
	}

	host := chooseBackend(m.schema)
	if host == "" {
		u.Errorf("missing address: %#v", m.schema)
		return nil, fmt.Errorf("Could not find Elasticsearch Host Address: %v", table)
	}
	tbl := datasource.NewTable(table, m.schema)

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

	tbl.AddField(datasource.NewField("_id", value.StringType, 24, "AUTOGEN"))
	tbl.AddField(datasource.NewField("type", value.StringType, 24, "tbd"))
	tbl.AddField(datasource.NewField("_score", value.NumberType, 24, "Created per Search By Elasticsearch"))

	tbl.AddValues([]driver.Value{"_id", "string", "NO", "PRI", "AUTOGEN", ""})
	tbl.AddValues([]driver.Value{"type", "string", "NO", "", nil, "tbd"})
	tbl.AddValues([]driver.Value{"_score", "float", "NO", "", nil, "Created per search"})

	buildEsFields(m.schema, tbl, jh, "", 0)

	m.schema.AddTable(tbl)

	return tbl, nil
}

func buildEsFields(s *datasource.SourceSchema, tbl *datasource.Table, jh u.JsonHelper, prefix string, depth int) {
	for field, _ := range jh {

		if h := jh.Helper(field); len(h) > 0 {
			jb, _ := json.Marshal(h)
			//jb, _ := json.MarshalIndent(h, " ", " ")
			fieldName := prefix + field
			var fld *datasource.Field
			//u.Infof("%v %v", fieldName, h)
			switch esType := h.String("type"); esType {
			case "boolean":
				tbl.AddValues([]driver.Value{fieldName, esType, "YES", "", nil, jb})
				//fld = mysql.NewField(fieldName, s.Db, s.Db, 1, mysql.MYSQL_TYPE_TINY)
				fld = datasource.NewField(fieldName, value.BoolType, 1, string(jb))
			case "string":
				tbl.AddValues([]driver.Value{fieldName, esType, "YES", "", nil, jb})
				//fld = mysql.NewField(fieldName, s.Db, s.Db, 512, mysql.MYSQL_TYPE_STRING)
				fld = datasource.NewField(fieldName, value.StringType, 512, string(jb))
			case "date":
				tbl.AddValues([]driver.Value{fieldName, esType, "YES", "", nil, jb})
				//fld = mysql.NewField(fieldName, s.Db, s.Db, 32, mysql.MYSQL_TYPE_DATETIME)
				fld = datasource.NewField(fieldName, value.TimeType, 4, string(jb))
			case "int", "long", "integer":
				tbl.AddValues([]driver.Value{fieldName, esType, "YES", "", nil, jb})
				//fld = mysql.NewField(fieldName, s.Db, s.Db, 64, mysql.MYSQL_TYPE_LONG)
				fld = datasource.NewField(fieldName, value.IntType, 8, string(jb))
			case "double", "float":
				tbl.AddValues([]driver.Value{fieldName, esType, "YES", "", nil, jb})
				//fld = mysql.NewField(fieldName, s.Db, s.Db, 64, mysql.MYSQL_TYPE_LONG)
				fld = datasource.NewField(fieldName, value.NumberType, 8, string(jb))
			case "nested", "object":
				tbl.AddValues([]driver.Value{fieldName, esType, "YES", "", nil, jb})
				//fld = mysql.NewField(fieldName, s.Db, s.Db, 2000, mysql.MYSQL_TYPE_BLOB)
				fld = datasource.NewField(fieldName, value.StringType, 2000, string(jb))
			default:
				tbl.AddValues([]driver.Value{fieldName, "object", "YES", "", nil, `{"type":"object"}`})
				//fld = mysql.NewField(fieldName, s.Db, s.Db, 2000, mysql.MYSQL_TYPE_BLOB)
				fld = datasource.NewField(fieldName, value.StringType, 2000, `{"type":"object"}`)
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
