package mongo

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	u "github.com/araddon/gou"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/plan"
	"github.com/araddon/qlbridge/schema"
	"github.com/araddon/qlbridge/value"
	"github.com/dataux/dataux/models"
)

var (
	// implement interfaces
	_ schema.DataSource = (*MongoDataSource)(nil)
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
//  - shared across all sessions/connections
type MongoDataSource struct {
	db        string
	databases []string
	conf      *models.Config
	schema    *schema.SourceSchema
	ctx       *plan.Context
	mu        sync.Mutex
	sess      *mgo.Session
	closed    bool
}

func NewMongoDataSource(schema *schema.SourceSchema, conf *models.Config) models.DataSource {
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

func (m *MongoDataSource) DataSource() schema.DataSource { return m }
func (m *MongoDataSource) Tables() []string              { return m.schema.Tables() }
func (m *MongoDataSource) Table(table string) (*schema.Table, error) {
	//u.LogTracef(u.WARN, "who calling me?")
	return m.loadTableSchema(table)
}

func (m *MongoDataSource) Open(collectionName string) (schema.SourceConn, error) {
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

func chooseBackend(source string, schema *schema.SourceSchema) string {
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

	//u.Infof("old session %p -> new session %p", m.sess, sess)
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
	//u.Debugf("found database names: %v", m.databases)
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
	//u.Debugf("found tables: %v", tables)
	return nil
}

func (m *MongoDataSource) loadTableSchema(table string) (*schema.Table, error) {

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
	tbl := schema.NewTable(table, m.schema)
	coll := m.sess.DB(m.db).C(table)
	colNames := make([]string, 0)
	errs := make(map[string]string)

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

			//[]string{"Field", "Type", "Collation", "Null", "Key", "Default", "Extra", "Privileges", "Comment"}
			//NewField(name string, valType value.ValueType, size int, nulls bool, defaultVal driver.Value, key, collation, description  string)
			switch val := iVal.(type) {
			case bson.ObjectId:
				//u.Debugf("found bson.ObjectId: %v='%v'", colName, val)
				tbl.AddField(schema.NewField(colName, value.StringType, 16, schema.NoNulls, nil, "PRI", "", "bson.ObjectID AUTOGEN"))
				//tbl.DescribeColumn([]driver.Value{colName, "char(24)", "NO", "PRI", "AUTOGEN", ""})
			case bson.M:
				//u.Debugf("found bson.M: %v='%v'", colName, val)
				tbl.AddField(schema.NewFieldBase(colName, value.MapValueType, 24, "bson.M"))
				//tbl.DescribeColumn([]driver.Value{colName, "text", "NO", "", "", "Nested Map Type, json object"})
			case map[string]interface{}:
				//u.Debugf("found map[string]interface{}: %v='%v'", colName, val)
				tbl.AddField(schema.NewFieldBase(colName, value.MapValueType, 24, "map[string]interface{}"))
				//tbl.DescribeColumn([]driver.Value{colName, "text", "NO", "", "", "Nested Map Type, json object"})
			case int:
				//u.Debugf("found int: %v='%v'", colName, val)
				tbl.AddField(schema.NewFieldBase(colName, value.IntType, 32, "int"))
				//tbl.DescribeColumn([]driver.Value{colName, "int(8)", "NO", "", "", "int"})
			case int64:
				//u.Debugf("found int64: %v='%v'", colName, val)
				tbl.AddField(schema.NewFieldBase(colName, value.IntType, 64, "long"))
				//tbl.DescribeColumn([]driver.Value{colName, "bigint", "NO", "", "", "long"})
			case float64:
				//u.Debugf("found float64: %v='%v'", colName, val)
				tbl.AddField(schema.NewFieldBase(colName, value.NumberType, 32, "float64"))
				//tbl.DescribeColumn([]driver.Value{colName, "float", "NO", "", "", "float64"})
			case string:
				//u.Debugf("found string: %v='%v'", colName, val)
				tbl.AddField(schema.NewFieldBase(colName, value.StringType, 255, "string"))
				//tbl.DescribeColumn([]driver.Value{colName, "varchar(255)", "NO", "", "", "string"})
			case bool:
				//u.Debugf("found string: %v='%v'", colName, val)
				tbl.AddField(schema.NewFieldBase(colName, value.BoolType, 1, "bool"))
				//tbl.DescribeColumn([]driver.Value{colName, "bool", "NO", "", "", "bool"})
			case time.Time:
				//u.Debugf("found time.Time: %v='%v'", colName, val)
				tbl.AddField(schema.NewFieldBase(colName, value.TimeType, 32, "datetime"))
				//tbl.DescribeColumn([]driver.Value{colName, "datetime", "NO", "", "", "datetime"})
			case *time.Time:
				//u.Debugf("found time.Time: %v='%v'", colName, val)
				tbl.AddField(schema.NewFieldBase(colName, value.TimeType, 32, "datetime"))
				//tbl.DescribeColumn([]driver.Value{colName, "datetime", "NO", "", "", "datetime"})
			case []uint8:
				// This is most likely binary data, json.RawMessage, or []bytes
				//u.Debugf("found []uint8: %v='%v'", colName, val)
				tbl.AddField(schema.NewFieldBase(colName, value.ByteSliceType, 1000, "[]byte"))
				//tbl.DescribeColumn([]driver.Value{colName, "binary", "NO", "", "", "Binary data:  []byte"})
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
					tbl.AddField(schema.NewFieldBase(colName, value.StringsType, 1000, "[]string"))
					//tbl.AddValues([]driver.Value{colName, "[]string", "NO", "", "", "[]string"})
					//tbl.DescribeColumn([]driver.Value{colName, "text", "NO", "", "", "json []string"})
				default:
					//u.Debugf("SEMI IMPLEMENTED:   found []interface{}: col:%s T:%T type:%v", colName, val, typ.String())
					tbl.AddField(schema.NewFieldBase(colName, value.SliceValueType, 1000, "[]value"))
					//tbl.AddValues([]driver.Value{colName, "[]value", "NO", "", "", "json []value"})
					//tbl.DescribeColumn([]driver.Value{colName, "text", "NO", "", "", "json []value"})
				}
			case nil:
				// ??
				//u.Warnf("could not infer from nil: colName  %v ", colName)
				errs[colName] = fmt.Sprintf("could not infer column type for %q because nil", colName)
				continue
			default:
				errs[colName] = fmt.Sprintf("not recognized type: v=%v T:%T", colName, iVal)
				continue
			}

			colNames = append(colNames, colName)
		}
	}
	if len(errs) > 0 {
		for _, errmsg := range errs {
			u.Warnf(errmsg)
		}
	}
	// buildMongoFields(s, tbl, jh, "", 0)
	tbl.SetColumns(colNames)
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
