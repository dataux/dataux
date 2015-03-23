package mongo

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"

	"gopkg.in/mgo.v2"
	//"gopkg.in/mgo.v2/bson"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
	"github.com/dataux/dataux/pkg/models"
)

/*
TODO:
 - This is not rorating amongst backends


*/
var (
	// Ensure our MongoDataSource is a datasource.DataSource type
	_ models.DataSource = (*MongoDataSource)(nil)

	features = &datasource.SourceFeatures{
		Scan:         true,
		Seek:         true,
		Where:        true,
		GroupBy:      true,
		Sort:         true,
		Aggregations: true,
	}
)

const (
	ListenerType = "mongo"
)

func init() {
	// We need to register our DataSource provider here
	models.DataSourceRegister("mongo", NewMongoDataSource)
}

type MongoDataSource struct {
	schema     *models.Schema
	conf       *models.Config
	schemaConf *models.SchemaConfig
	mu         sync.Mutex
	sess       *mgo.Session
	closed     bool
}

func NewMongoDataSource(schema *models.Schema, conf *models.Config) models.DataSource {
	m := MongoDataSource{}
	m.schema = schema
	m.schemaConf = schema.Conf
	m.conf = conf
	return &m
}

func (m *MongoDataSource) Init() error {

	u.Infof("Init:  %#v", m.schemaConf)
	if m.schemaConf == nil {
		return fmt.Errorf("Schema conf not found")
	}

	if err := m.connect(); err != nil {
		return err
	}
	u.Debugf("Init() mongo schema P=%p", m.schema)
	if err := m.findMongoNodes(); err != nil {
		u.Errorf("could not init mgo: %v", err)
		return err
	}

	if err := m.loadTableNames(); err != nil {
		u.Errorf("could not load mgo tables: %v", err)
		return err
	}
	if m.schema != nil {
		u.Debugf("Post Init() mongo schema P=%p tblct=%d", m.schema, len(m.schema.Tables))
	}
	return nil
}

func (m *MongoDataSource) Close() error {
	u.Infof("Closing MongoDataSource %p", m)
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.closed && m.sess != nil {
		u.Infof("Closing MongoDataSource %p session %p", m, m.sess)
		m.sess.Close()
	} else {
		u.Infof("Told to close mgomodelstore %p but session=%p closed=%t", m, m.closed)
	}
	m.closed = true
	return nil
}

func (m *MongoDataSource) connect() error {
	u.Infof("connecting MongoDataSource to host: %v: db: %v", m.schemaConf.Address, m.schemaConf.DB)
	m.mu.Lock()
	defer m.mu.Unlock()

	// TODO
	//host := s.ChooseBackend()
	sess, err := mgo.Dial(m.schemaConf.Address)
	if err != nil {
		u.Errorf("Could not connect to mongo: %v", err)
		return err
	}

	// We depend on read-your-own-writes consistency in several places
	sess.SetMode(mgo.Strong, true)

	// Unbelievably, you have to actually enable error checking
	sess.SetSafe(&mgo.Safe{}) // copied from the mgo package docs

	sess.SetPoolLimit(1024)

	// Close the existing session if there is one
	if m.sess != nil {
		u.Infof("reconnecting MongoDataSource %p (closing old session)", m)
		m.sess.Close()
	}

	u.Infof("old session %p -> new session %p", m.sess, sess)
	m.sess = sess

	db := sess.DB(m.schemaConf.DB)
	if db == nil {
		return fmt.Errorf("Database %v not found", m.schemaConf.DB)
	}

	return nil
}

func (m *MongoDataSource) SourceTask(stmt *expr.SqlSelect) (models.SourceTask, error) {

	u.Debugf("get sourceTask for %v", stmt)
	tblName := strings.ToLower(stmt.From[0].Name)

	tbl, _ := m.schema.Table(tblName)
	if tbl == nil {
		u.Errorf("Could not find table for '%s'.'%s'", m.schema.Db, tblName)
		return nil, fmt.Errorf("Could not find '%v'.'%v' schema", m.schema.Db, tblName)
	}

	es := NewSqlToMgo(tbl)
	u.Debugf("SqlToMgo: %#v", es)
	resp, err := es.Query(stmt)
	if err != nil {
		u.Error(err)
		return nil, err
	}

	return resp, nil
}

func (m *MongoDataSource) Features() *datasource.SourceFeatures { return features }

func (m *MongoDataSource) Table(table string) (*models.Table, error) {
	u.Debugf("get table for %s", table)
	return m.loadTableSchema(table)
}

// Load only table names, not full schema
func (m *MongoDataSource) loadTableNames() error {

	tables, err := m.sess.DatabaseNames()
	if err != nil {
		return err
	}
	sort.Strings(tables)
	m.schema.TableNames = tables
	u.Debugf("found tables: %v", m.schema.TableNames)

	return nil
}

func (m *MongoDataSource) loadTableSchema(table string) (*models.Table, error) {

	if m.schema == nil {
		return nil, fmt.Errorf("no schema in use")
	}
	// check cache first
	if tbl, ok := m.schema.Tables[table]; ok {
		return tbl, nil
	}

	s := m.schema
	tbl := models.NewTable(table, s)

	coll := m.sess.DB(m.schemaConf.DB).C(table)
	var dataType []map[string]interface{}
	if err := coll.Find(nil).All(&dataType); err != nil {
		u.Errorf("could not query collection")
	}
	for _, dt := range dataType {
		u.Debugf("found col: %#v", dt)
	}

	// tbl.AddField(models.NewField("_id", value.StringType, 24, "AUTOGEN"))
	// tbl.AddField(models.NewField("type", value.StringType, 24, "tbd"))
	// tbl.AddField(models.NewField("_score", value.NumberType, 24, "Created per Search By Elasticsearch"))

	// tbl.AddValues([]driver.Value{"_id", "string", "NO", "PRI", "AUTOGEN", ""})
	// tbl.AddValues([]driver.Value{"type", "string", "NO", "", nil, "tbd"})
	// tbl.AddValues([]driver.Value{"_score", "float", "NO", "", nil, "Created per search"})

	// buildMongoFields(s, tbl, jh, "", 0)
	m.schema.Tables[table] = tbl

	return tbl, nil
}

func buildMongoFields(s *models.Schema, tbl *models.Table, jh u.JsonHelper, prefix string, depth int) {
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
					buildMongoFields(s, tbl, props, fieldName+".", depth+1)
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

func (m *MongoDataSource) findMongoNodes() error {

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
			u.Debugf("adding node: %s", be.String())
			//m.nodes[be.Name] = n
		}
	}

	return nil
}
