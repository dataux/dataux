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

	"github.com/araddon/qlbridge/schema"
	"github.com/araddon/qlbridge/value"
)

var (
	// Ensure we implement source interface
	_ schema.Source = (*Source)(nil)
)

const (
	// SourceType for this mongo source.
	SourceType = "mongo"
)

func init() {
	// We need to register our DataSource provider here
	schema.RegisterSourceType(SourceType, NewSource())
}

// Source Mongo Data Source implements qlbridge DataSource interfaces to mongo server
// - singleton shared across all sessions/connections
// - creates connections by mgo.Session.Clone()
type Source struct {
	db             string   // the underlying mongo database name
	databases      []string // all available database names from this mongo instance
	tables         []string // tables
	srcschema      *schema.Schema
	mu             sync.Mutex
	sess           *mgo.Session
	closed         bool
	loadedSchema   bool
	tablesNotFound map[string]string
}

// NewSource mongo source.
func NewSource() schema.Source {
	return &Source{tablesNotFound: make(map[string]string)}
}

// Init initilize this source.
func (m *Source) Init() {}

// Setup this source.
func (m *Source) Setup(ss *schema.Schema) error {

	u.Debugf("Setup()")
	if m.srcschema != nil {
		return nil
	}

	m.srcschema = ss
	if ss.Conf != nil && len(ss.Conf.Partitions) > 0 {

	}
	m.db = strings.ToLower(ss.Name)

	u.Infof("Init:  %#v", m.srcschema.Conf)
	if m.srcschema.Conf == nil {
		return fmt.Errorf("Schema conf not found")
	}

	// This will return an error if the database name we are using is not found
	if err := m.connect(); err != nil {
		return err
	}

	if m.srcschema != nil {
		//u.Debugf("Post Init() mongo srcschema P=%p tblct=%d", m.srcschema, len(m.srcschema.Tables()))
	}

	return m.loadSchema()
}

func (m *Source) Close() error {
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

// Tables list of tables
func (m *Source) Tables() []string { return m.tables }

// Table get single table schema.
func (m *Source) Table(table string) (*schema.Table, error) {
	if !m.loadedSchema {
		m.loadedSchema = true
		return m.loadTableSchema(table)
	}
	if _, alreadyTried := m.tablesNotFound[table]; !alreadyTried {
		m.tablesNotFound[table] = ""
		tbl, err := m.loadTableSchema(table)
		if err == nil {
			delete(m.tablesNotFound, table)
		}
		return tbl, err
	}

	return nil, schema.ErrNotFound
}

// Open connection.
func (m *Source) Open(collectionName string) (schema.Conn, error) {
	//u.Debugf("Open(%v)", collectionName)
	tbl, err := m.srcschema.Table(collectionName)
	if err != nil {
		return nil, err
	}
	if tbl == nil {
		u.Errorf("Could not find table for '%s'.'%s'", m.srcschema.Name, collectionName)
		return nil, fmt.Errorf("Could not find '%v'.'%v' schema", m.srcschema.Name, collectionName)
	}

	//u.Debugf("creating sqltomgo %v", tbl.Name)
	mgoSource := NewSqlToMgo(tbl, m.sess.Clone())
	//u.Debugf("SqlToMgo: %T  %#v", mgoSource, mgoSource)
	return mgoSource, nil
}

func (m *Source) loadSchema() error {

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

// TODO:  this is horrible, should use mgo's built in gossip with mongo for cluster info
func chooseBackend(source string, schema *schema.Schema) string {
	//u.Infof("check backends: %v", len(schema.Nodes))
	for _, node := range schema.Conf.Nodes {
		//u.Debugf("check node:%q =? %+v", source, node)
		if node.Source == source {
			//u.Debugf("found node: %+v", node)
			// TODO:  implement real balancer
			return node.Address
		}
	}
	return ""
}

func (m *Source) connect() error {

	host := chooseBackend(m.db, m.srcschema)

	u.Debugf("connecting MongoDataSource: host='%s'  conf=%#v", host, m.srcschema.Conf)
	m.mu.Lock()
	defer m.mu.Unlock()

	//host = "localhost:27018"

	sess, err := mgo.Dial(host)
	if err != nil {
		u.Errorf("Could not connect to mongo: host=%q  err=%v", host, err)
		return err
	}

	sess.SetMode(mgo.Strong, true)

	sess.SetSafe(&mgo.Safe{}) // copied from the mgo package docs

	sess.SetPoolLimit(1024)

	// Close the existing session if there is one
	if m.sess != nil {
		u.Infof("reconnecting MongoDataSource %p (closing old session)", m)
		m.sess.Close()
	}

	//u.Infof("old session %p -> new session %p", m.sess, sess)
	m.sess = sess

	db := sess.DB(m.srcschema.Name)
	if db == nil {
		return fmt.Errorf("Database %v not found", m.srcschema.Name)
	}

	return nil
}

func (m *Source) loadDatabases() error {

	dbs, err := m.sess.DatabaseNames()
	if err != nil {
		return err
	}
	sort.Strings(dbs)
	m.databases = dbs
	//u.Debugf("found database names: %v", m.databases)
	found := false
	for _, db := range dbs {
		if strings.ToLower(db) == strings.ToLower(m.srcschema.Name) {
			found = true
		}
	}
	if !found {
		u.Warnf("could not find database: %q", m.srcschema.Name)
		return fmt.Errorf("Could not find that database: %v", m.srcschema.Name)
	}

	return nil
}

// Load only table/collection names, not full schema
func (m *Source) loadTableNames() error {

	db := m.sess.DB(m.db)
	tables, err := db.CollectionNames()
	if err != nil {
		return err
	}
	sort.Strings(tables)
	m.tables = tables
	// for _, tableName := range tables {
	// 	u.Debugf("ss:%p AddTableName", m.srcschema)
	// 	//m.srcschema.AddTableName(tableName)
	// 	u.Debugf("ss:%p AFTER AddTableName", m.srcschema)
	// 	//u.Debugf("found table %q", tableName)
	// }
	//u.Debugf("found tables: %v", tables)
	return nil
}

func (m *Source) loadTableSchema(table string) (*schema.Table, error) {

	if m.srcschema == nil {
		return nil, fmt.Errorf("no schema in use")
	}

	//u.Infof("Loading Table Schema %q", table)
	tbl := schema.NewTable(table)
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
				tbl.AddField(schema.NewField(colName, value.StringType, 16, schema.NoNulls, nil, "PRI", "", "bson.ObjectID AUTOGEN"))
			case bson.M:
				tbl.AddField(schema.NewFieldBase(colName, value.MapValueType, 24, "bson.M"))
			case map[string]interface{}:
				tbl.AddField(schema.NewFieldBase(colName, value.MapValueType, 24, "map[string]interface{}"))
			case int:
				tbl.AddField(schema.NewFieldBase(colName, value.IntType, 32, "int"))
			case int64:
				tbl.AddField(schema.NewFieldBase(colName, value.IntType, 64, "long"))
			case float64:
				tbl.AddField(schema.NewFieldBase(colName, value.NumberType, 32, "float64"))
			case string:
				tbl.AddField(schema.NewFieldBase(colName, value.StringType, 255, "string"))
			case bool:
				tbl.AddField(schema.NewFieldBase(colName, value.BoolType, 1, "bool"))
			case time.Time:
				tbl.AddField(schema.NewFieldBase(colName, value.TimeType, 32, "datetime"))
			case *time.Time:
				tbl.AddField(schema.NewFieldBase(colName, value.TimeType, 32, "datetime"))
			case []uint8:
				// This is most likely binary data, json.RawMessage, or []bytes
				tbl.AddField(schema.NewFieldBase(colName, value.ByteSliceType, 1000, "[]byte"))
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
				default:
					//u.Debugf("SEMI IMPLEMENTED:   found []interface{}: col:%s T:%T type:%v", colName, val, typ.String())
					tbl.AddField(schema.NewFieldBase(colName, value.SliceValueType, 1000, "[]value"))
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
	tbl.SetColumns(colNames)
	//m.srcschema.AddTable(tbl)

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
