package elasticsearch

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"sort"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/value"
	"github.com/dataux/dataux/pkg/models"
)

// Provide a DataSource provider,
//  - provide schema info
//  - connection manager

type ElasticsearchDataSource struct {
	schema     *models.Schema
	conf       *models.Config
	schemaConf *models.SchemaConfig
}

func NewElasticsearchDataSource(schema *models.Schema, conf *models.Config) models.DataSource {
	es := ElasticsearchDataSource{}
	es.schema = schema
	es.conf = conf
	return &es
}

func (m *ElasticsearchDataSource) Init() error {

	u.Debugf("Init()")
	if err := m.findEsNodes(); err != nil {
		u.Errorf("could not init es: %v", err)
		return err
	}

	if err := m.loadTableNames(); err != nil {
		u.Errorf("could not load es tables: %v", err)
		return err
	}

	return nil
}

func (m *ElasticsearchDataSource) Close() error { return nil }

func (m *ElasticsearchDataSource) Table(table string) (*models.Table, error) {
	u.Debugf("get table for %s", table)
	return m.loadTableSchema(table)
}

// Load only table names, not full schema
func (m *ElasticsearchDataSource) loadTableNames() error {

	jh, err := u.JsonHelperHttp("GET", "http://localhost:9200/_aliases", nil)
	if err != nil {
		u.Error("error on es read: %v", err)
		return err
	}
	//u.Debugf("resp: %v", jh)
	tables := []string{}
	for alias, _ := range jh {
		tables = append(tables, alias)
	}
	sort.Strings(tables)

	// move this to an initial load?
	if m.schema == nil {
		u.Infof("no schema? %v")
	}
	m.schema.TableNames = tables
	u.Debugf("found tables: %v", m.schema.TableNames)

	return nil
}

func (m *ElasticsearchDataSource) loadTableSchema(table string) (*models.Table, error) {

	if m.schema == nil {
		return nil, fmt.Errorf("no schema in use")
	}
	// check cache first
	if tbl, ok := m.schema.Tables[table]; ok {
		return tbl, nil
	}

	s := m.schema
	host := s.ChooseBackend()
	if m.schema.Address == "" {
		u.Errorf("missing address: %#v", m.schema)
	}
	tbl := models.NewTable(table, m.schema)

	indexUrl := fmt.Sprintf("%s/%s/_mapping", host, tbl.Name)
	respJh, err := u.JsonHelperHttp("GET", indexUrl, nil)
	if err != nil {
		u.Error("error on es read: url=%v  err=%v", indexUrl, err)
	}
	u.Debugf("url: %v", indexUrl)
	respJh = respJh.Helper(table + ".mappings")
	respKeys := respJh.Keys()
	//u.Infof("keys:%v  resp:%v", respKeys, respJh)
	if len(respKeys) < 1 {
		u.Errorf("could not get data? %v   %v", indexUrl, respJh)
		return nil, fmt.Errorf("Could not process desribe")
	}
	indexType := "user"
	for _, key := range respKeys {
		if key != "_default_" {
			indexType = key
			break
		}
	}

	jh := respJh.Helper(indexType)
	//u.Debugf("resp: %v", jh)
	jh = jh.Helper("properties")

	tbl.AddField(models.NewField("_id", value.StringType, 24, "AUTOGEN"))
	tbl.AddField(models.NewField("type", value.StringType, 24, "tbd"))
	tbl.AddField(models.NewField("_score", value.NumberType, 24, "Created per Search By Elasticsearch"))

	tbl.AddValues([]driver.Value{"_id", "string", "NO", "PRI", "AUTOGEN", ""})
	tbl.AddValues([]driver.Value{"type", "string", "NO", "", nil, "tbd"})
	tbl.AddValues([]driver.Value{"_score", "float", "NO", "", nil, "Created per search"})

	buildEsFields(s, tbl, jh, "", 0)
	m.schema.Tables[table] = tbl

	return tbl, nil
}

func buildEsFields(s *models.Schema, tbl *models.Table, jh u.JsonHelper, prefix string, depth int) {
	for field, _ := range jh {

		if h := jh.Helper(field); len(h) > 0 {
			jb, _ := json.Marshal(h)
			//jb, _ := json.MarshalIndent(h, " ", " ")
			fieldName := prefix + field
			var fld *models.Field
			//u.Infof("%v %v", fieldName, h)
			switch esType := h.String("type"); esType {
			case "boolean":
				tbl.AddValues([]driver.Value{fieldName, esType, "YES", "", nil, jb})
				//fld = mysql.NewField(fieldName, s.Db, s.Db, 1, mysql.MYSQL_TYPE_TINY)
				fld = models.NewField(fieldName, value.BoolType, 1, string(jb))
			case "string":
				tbl.AddValues([]driver.Value{fieldName, esType, "YES", "", nil, jb})
				//fld = mysql.NewField(fieldName, s.Db, s.Db, 512, mysql.MYSQL_TYPE_STRING)
				fld = models.NewField(fieldName, value.StringType, 512, string(jb))
			case "date":
				tbl.AddValues([]driver.Value{fieldName, esType, "YES", "", nil, jb})
				//fld = mysql.NewField(fieldName, s.Db, s.Db, 32, mysql.MYSQL_TYPE_DATETIME)
				fld = models.NewField(fieldName, value.TimeType, 4, string(jb))
			case "int", "long", "integer":
				tbl.AddValues([]driver.Value{fieldName, esType, "YES", "", nil, jb})
				//fld = mysql.NewField(fieldName, s.Db, s.Db, 64, mysql.MYSQL_TYPE_LONG)
				fld = models.NewField(fieldName, value.IntType, 8, string(jb))
			case "nested":
				tbl.AddValues([]driver.Value{fieldName, esType, "YES", "", nil, jb})
				//fld = mysql.NewField(fieldName, s.Db, s.Db, 2000, mysql.MYSQL_TYPE_BLOB)
				fld = models.NewField(fieldName, value.StringType, 2000, string(jb))
			default:
				tbl.AddValues([]driver.Value{fieldName, "object", "YES", "", nil, `{"type":"object"}`})
				//fld = mysql.NewField(fieldName, s.Db, s.Db, 2000, mysql.MYSQL_TYPE_BLOB)
				fld = models.NewField(fieldName, value.StringType, 2000, `{"type":"object"}`)
				props := h.Helper("properties")
				if len(props) > 0 {
					buildEsFields(s, tbl, props, fieldName+".", depth+1)
				} else {
					u.Debugf("unknown type: %v", string(jb))
				}

			}
			if fld != nil {
				tbl.AddField(fld)
			}

		}
	}
}

func (m *ElasticsearchDataSource) findEsNodes() error {

	//m.nodes = make(map[string]*Node)

	for _, be := range m.conf.Sources {
		if be.SourceType == "" {
			for _, schemaConf := range m.conf.Schemas {
				for _, bename := range schemaConf.Nodes {
					if bename == be.Name {
						be.SourceType = schemaConf.SourceType
					}
				}
			}
		}
		if be.SourceType == ListenerType {
			// if _, ok := m.nodes[be.Name]; ok {
			// 	return fmt.Errorf("duplicate node '%s'", be.Name)
			// }

			// n, err := m.startMysqlNode(be)
			// if err != nil {
			// 	return err
			// }

			u.Debugf("adding node: %s", be.String())
			//m.nodes[be.Name] = n
		}
	}

	return nil
}
