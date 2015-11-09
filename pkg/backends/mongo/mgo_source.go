package mongo

import (
	"database/sql/driver"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/value"
	"github.com/dataux/dataux/pkg/models"
)

var (
	// implement interfaces
	_ datasource.DataSource = (*MongoDataSource)(nil)
)

const (
	ListenerType = "mongo"
)

func init() {
	// We need to register our DataSource provider here
	models.DataSourceRegister("mongo", NewMongoDataSource)
}

// Mongo Data Source implements qlbridge DataSource interfaces
//  to a backend mongo server
type MongoDataSource struct {
	db        string
	databases []string
	conf      *models.Config
	schema    *datasource.SourceSchema
	mu        sync.Mutex
	sess      *mgo.Session
	closed    bool
}

func NewMongoDataSource(schema *datasource.SourceSchema, conf *models.Config) models.DataSource {
	m := MongoDataSource{}
	m.schema = schema
	m.conf = conf
	m.db = strings.ToLower(schema.Name)
	// Register our datasource.Datasources in registry
	if err := m.Init(); err != nil {
		u.Errorf("Could not open Mongo datasource %v", err)
	}
	datasource.Register("mongo", &m)
	return &m
}

func (m *MongoDataSource) Init() error {

	//u.Infof("Init:  %#v", m.schema.Conf)
	if m.schema.Conf == nil {
		return fmt.Errorf("Schema conf not found")
	}

	// This will return an error if the database name we are using nis not found
	if err := m.connect(); err != nil {
		return err
	}

	if m.schema != nil {
		//u.Debugf("Post Init() mongo schema P=%p tblct=%d", m.schema, len(m.schema.Tables()))
	}

	return m.loadSchema()
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

func (m *MongoDataSource) DataSource() datasource.DataSource { return m }
func (m *MongoDataSource) Tables() []string                  { return m.schema.Tables() }
func (m *MongoDataSource) Table(table string) (*datasource.Table, error) {
	return m.loadTableSchema(table)
}

func (m *MongoDataSource) Open(collectionName string) (datasource.SourceConn, error) {
	//u.Debugf("Open(%v)", collectionName)
	tbl, err := m.schema.Table(collectionName)
	if err != nil {
		return nil, err
	}
	if tbl == nil {
		u.Errorf("Could not find table for '%s'.'%s'", m.schema.Name, collectionName)
		return nil, fmt.Errorf("Could not find '%v'.'%v' schema", m.schema.Name, collectionName)
	}

	mgoSource := NewSqlToMgo(tbl, m.sess.Clone())
	//u.Debugf("SqlToMgo: %T  %#v", mgoSource, mgoSource)
	return mgoSource, nil
}

func (m *MongoDataSource) loadSchema() error {

	if err := m.loadDatabases(); err != nil {
		u.Errorf("could not load mongo databases: %v", err)
		return err
	}

	if err := m.loadTableNames(); err != nil {
		u.Errorf("could not load mongo tables: %v", err)
		return err
	}
	return nil
}

func chooseBackend(source string, schema *datasource.SourceSchema) string {
	for _, node := range schema.Nodes {
		u.Infof("check node:%q =? %+v", source, node)
		if node.Source == source {
			u.Debugf("found node: %+v", node)
			// TODO:  implement real balancer
			return node.Address
		}
	}
	return ""
}

func (m *MongoDataSource) connect() error {

	host := chooseBackend(m.db, m.schema)

	u.Infof("connecting MongoDataSource: host='%s'  conf=%#v", host, m.schema.Conf)
	m.mu.Lock()
	defer m.mu.Unlock()

	sess, err := mgo.Dial(host)
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

	db := sess.DB(m.schema.Name)
	if db == nil {
		return fmt.Errorf("Database %v not found", m.schema.Name)
	}

	return nil
}

func (m *MongoDataSource) loadDatabases() error {

	dbs, err := m.sess.DatabaseNames()
	if err != nil {
		return err
	}
	sort.Strings(dbs)
	m.databases = dbs
	u.Debugf("found database names: %v", m.databases)
	found := false
	for _, db := range dbs {
		if strings.ToLower(db) == strings.ToLower(m.schema.Name) {
			found = true
		}
	}
	if !found {
		u.Warnf("could not find database: %q", m.schema.Name)
		return fmt.Errorf("Could not find that database: %v", m.schema.Name)
	}

	return nil
}

// Load only table/collection names, not full schema
func (m *MongoDataSource) loadTableNames() error {

	db := m.sess.DB(m.db)
	tables, err := db.CollectionNames()
	if err != nil {
		return err
	}
	for _, tableName := range tables {
		m.schema.AddTableName(tableName)
	}
	u.Debugf("found tables: %v", tables)
	return nil
}

func (m *MongoDataSource) loadTableSchema(table string) (*datasource.Table, error) {

	if m.schema == nil {
		return nil, fmt.Errorf("no schema in use")
	}
	/*
		TODO:
			- Need to read the indexes, and include that info
			- make recursive
			- allow future columns to get refreshed/discovered at runtime?
			- Resolve differences between object's/schemas? having different fields, types, nested
				- use new structure for Observations
			- shared pkg for data-inspection, data builder
	*/
	tbl := datasource.NewTable(table, m.schema)
	coll := m.sess.DB(m.db).C(table)

	var sampleRows []map[string]interface{}
	if err := coll.Find(nil).Limit(30).All(&sampleRows); err != nil {
		u.Errorf("could not query collection")
	}
	//u.Debugf("loading %s", table)
	for _, sampleRow := range sampleRows {
		//u.Infof("%#v", sampleRow)
		for colName, iVal := range sampleRow {

			colName = strings.ToLower(colName)
			//u.Debugf("found col: %s %T=%v", colName, iVal, iVal)
			if tbl.HasField(colName) {
				continue
			}
			switch val := iVal.(type) {
			case bson.ObjectId:
				//u.Debugf("found bson.ObjectId: %v='%v'", colName, val)
				tbl.AddField(datasource.NewField(colName, value.StringType, 24, "bson.ObjectID AUTOGEN"))
				tbl.AddValues([]driver.Value{colName, "string", "NO", "PRI", "AUTOGEN", ""})
			case bson.M:
				//u.Debugf("found bson.M: %v='%v'", colName, val)
				tbl.AddField(datasource.NewField(colName, value.MapValueType, 24, "bson.M"))
				tbl.AddValues([]driver.Value{colName, "object", "NO", "", "", "Nested Map Type"})
			case map[string]interface{}:
				//u.Debugf("found map[string]interface{}: %v='%v'", colName, val)
				tbl.AddField(datasource.NewField(colName, value.MapValueType, 24, "map[string]interface{}"))
				tbl.AddValues([]driver.Value{colName, "object", "NO", "", "", "Nested Map Type"})
			case int:
				//u.Debugf("found int: %v='%v'", colName, val)
				tbl.AddField(datasource.NewField(colName, value.IntType, 32, "int"))
				tbl.AddValues([]driver.Value{colName, "int", "NO", "", "", "int"})
			case int64:
				//u.Debugf("found int64: %v='%v'", colName, val)
				tbl.AddField(datasource.NewField(colName, value.IntType, 32, "long"))
				tbl.AddValues([]driver.Value{colName, "long", "NO", "", "", "long"})
			case float64:
				//u.Debugf("found float64: %v='%v'", colName, val)
				tbl.AddField(datasource.NewField(colName, value.NumberType, 32, "float64"))
				tbl.AddValues([]driver.Value{colName, "float64", "NO", "", "", "float64"})
			case string:
				//u.Debugf("found string: %v='%v'", colName, val)
				tbl.AddField(datasource.NewField(colName, value.StringType, 32, "string"))
				tbl.AddValues([]driver.Value{colName, "string", "NO", "", "", "string"})
			case bool:
				//u.Debugf("found string: %v='%v'", colName, val)
				tbl.AddField(datasource.NewField(colName, value.BoolType, 1, "bool"))
				tbl.AddValues([]driver.Value{colName, "bool", "NO", "", "", "bool"})
			case time.Time:
				//u.Debugf("found time.Time: %v='%v'", colName, val)
				tbl.AddField(datasource.NewField(colName, value.TimeType, 32, "datetime"))
				tbl.AddValues([]driver.Value{colName, "datetime", "NO", "", "", "datetime"})
			case *time.Time:
				//u.Debugf("found time.Time: %v='%v'", colName, val)
				tbl.AddField(datasource.NewField(colName, value.TimeType, 32, "datetime"))
				tbl.AddValues([]driver.Value{colName, "datetime", "NO", "", "", "datetime"})
			case []uint8:
				// This is most likely binary data, json.RawMessage, or []bytes
				//u.Debugf("found []uint8: %v='%v'", colName, val)
				tbl.AddField(datasource.NewField(colName, value.ByteSliceType, 24, "[]byte"))
				tbl.AddValues([]driver.Value{colName, "binary", "NO", "", "", "Binary data:  []byte"})
			case []string:
				u.Warnf("NOT IMPLEMENTED:  found []string %v='%v'", colName, val)
			case []interface{}:
				// We don't currently allow infinite recursion.  Probably should same as ES with
				//  a prefix
				//u.Debugf("SEMI IMPLEMENTED:   found []interface{}: %v='%v'", colName, val)
				typ := value.NilType
				for _, sliceVal := range val {
					typ = discoverType(sliceVal)
				}
				switch typ {
				case value.StringType:
					tbl.AddField(datasource.NewField(colName, value.StringsType, 24, "[]string"))
					tbl.AddValues([]driver.Value{colName, "[]string", "NO", "", "", "[]string"})
				default:
					u.Infof("SEMI IMPLEMENTED:   found []interface{}: %v='%v'  %T %v", colName, val, val, typ.String())
					tbl.AddField(datasource.NewField(colName, value.SliceValueType, 24, "[]value"))
					tbl.AddValues([]driver.Value{colName, "[]value", "NO", "", "", "[]value"})
				}

			default:
				if iVal != nil {
					u.Warnf("not recognized type: %v %T", colName, iVal)
				} else {
					u.Warnf("could not infer from nil: %v", colName)
				}
			}
		}
	}

	// buildMongoFields(s, tbl, jh, "", 0)
	m.schema.AddTable(tbl)

	return tbl, nil
}

func discoverType(iVal interface{}) value.ValueType {

	switch iVal.(type) {
	case bson.ObjectId:
		return value.StringType
	case bson.M:
		return value.MapValueType
	case map[string]interface{}:
		return value.MapValueType
	case int:
		return value.IntType
	case int64:
		return value.IntType
	case float64:
		return value.NumberType
	case string:
		return value.StringType
	case time.Time:
		return value.TimeType
	case *time.Time:
		return value.TimeType
	case []uint8:
		return value.ByteSliceType
	case []string:
		return value.StringsType
	case []interface{}:
		return value.SliceValueType
	default:
		u.Warnf("not recognized type:  %T %#v", iVal, iVal)
	}
	return value.NilType
}
