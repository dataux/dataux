package datastore

import (
	"database/sql/driver"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/context"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/cloud"
	"google.golang.org/cloud/datastore"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
	"github.com/dataux/dataux/pkg/models"
)

const (
	DataSourceLabel = "google-datastore"
)

var (
	GoogleJwt     *string = flag.String("googlejwt", os.Getenv("GOOGLEJWT"), "Path to google JWT oauth token file")
	GoogleProject *string = flag.String("googleproject", os.Getenv("GOOGLEPROJECT"), "Google Datastore Project Name")

	ErrNoSchema = fmt.Errorf("No schema or configuration exists")

	// Ensure our Google DataStore implements datasource.DataSource interface
	_ datasource.DataSource = (*GoogleDSDataSource)(nil)
	_ datasource.Upsert     = (*GoogleDSDataSource)(nil)
	_ datasource.Deletion   = (*GoogleDSDataSource)(nil)
	//_ datasource.Scanner    = (*ResultReader)(nil)
	// source
	_ models.DataSource = (*GoogleDSDataSource)(nil)
)

func init() {
	// We need to register our DataSource provider here
	models.DataSourceRegister(DataSourceLabel, NewGoogleDataStoreDataSource)
}

// Google Datastore Data Source, is a singleton, non-threadsafe connection
//  to a backend mongo server
type GoogleDSDataSource struct {
	db        string
	namespace string
	databases []string
	dsCtx     context.Context
	conf      *models.Config
	schema    *models.SourceSchema
	mu        sync.Mutex
	closed    bool
}

func NewGoogleDataStoreDataSource(schema *models.SourceSchema, conf *models.Config) models.DataSource {
	m := GoogleDSDataSource{}
	m.schema = schema
	m.conf = conf
	m.db = strings.ToLower(schema.Db)
	// Register our datasource.Datasources in registry
	m.Init()
	u.Infof("datasource.Register: %v", DataSourceLabel)
	datasource.Register(DataSourceLabel, &m)
	return &m
}

func (m *GoogleDSDataSource) Init() error {

	//u.Infof("Init:  %#v", m.schema.Conf)
	if m.schema.Conf == nil {
		return fmt.Errorf("Schema conf not found")
	}

	// This will return an error if the database name we are using nis not found
	if err := m.connect(); err != nil {
		return err
	}

	return m.loadSchema()
}

func (m *GoogleDSDataSource) loadSchema() error {

	// Load a list of projects?  Namespaces?
	// if err := m.loadNamespaces(); err != nil {
	// 	u.Errorf("could not load google datastore namespace: %v", err)
	// 	return err
	// }

	if err := m.loadTableNames(); err != nil {
		u.Errorf("could not load google datastore kinds: %v", err)
		return err
	}
	return nil
}

func (m *GoogleDSDataSource) Close() error {
	u.Infof("Closing GoogleDSDataSource %p", m)
	m.mu.Lock()
	defer m.mu.Unlock()

	m.closed = true
	return nil
}

func (m *GoogleDSDataSource) connect() error {
	//host := m.schema.ChooseBackend()
	//u.Infof("connecting GoogleDSDataSource: host='%s'  conf=%#v", host, m.schema.Conf)
	m.mu.Lock()
	defer m.mu.Unlock()

	jsonKey, err := ioutil.ReadFile(*GoogleJwt)
	if err != nil {
		u.Errorf("Could not open Google Auth Token JWT file %v", err)
		os.Exit(1)
	}

	conf, err := google.JWTConfigFromJSON(
		jsonKey,
		datastore.ScopeDatastore,
		datastore.ScopeUserEmail,
	)
	if err != nil {
		u.Errorf("could not use google datastore JWT token: %v", err)
		return err
	}
	m.dsCtx = cloud.NewContext(*GoogleProject, conf.Client(oauth2.NoContext))

	return nil
}

func (m *GoogleDSDataSource) DataSource() datasource.DataSource {
	return m
}
func (m *GoogleDSDataSource) Tables() []string {
	return m.schema.TableNames
}

func (m *GoogleDSDataSource) Open(tableName string) (datasource.SourceConn, error) {
	u.Debugf("Open(%v)", tableName)
	if m.schema == nil {
		u.Warnf("no schema?")
		return nil, nil
	}
	tableName = strings.ToLower(tableName)
	tbl := m.schema.Tables[tableName]
	if tbl == nil {
		u.Errorf("Could not find table for '%s'.'%s'", m.schema.Db, tableName)
		return nil, fmt.Errorf("Could not find '%v'.'%v' schema", m.schema.Db, tableName)
	}

	//es := NewSqlToMgo(tbl, m.sess)
	//u.Debugf("SqlToMgo: %#v", es)
	// resp, err := es.Query(stmt, m.sess)
	// if err != nil {
	// 	u.Error(err)
	// 	return nil, err
	// }
	//return es, nil
	sqlDs := NewSqlToDatstore(tbl, m.dsCtx)
	return sqlDs, nil

	return nil, nil
}

func (m *GoogleDSDataSource) SourceTask(stmt *expr.SqlSelect) (models.SourceTask, error) {

	u.Debugf("get sourceTask for %v", stmt)
	tblName := strings.ToLower(stmt.From[0].Name)

	tbl := m.schema.Tables[tblName]
	if tbl == nil {
		u.Errorf("Could not find table for '%s'.'%s'", m.schema.Db, tblName)
		return nil, fmt.Errorf("Could not find '%v'.'%v' schema", m.schema.Db, tblName)
	}

	sqlDs := NewSqlToDatstore(tbl, m.dsCtx)
	//u.Debugf("SqlToDatstore: %#v", sqlDs)
	resp, err := sqlDs.Query(stmt)
	if err != nil {
		u.Errorf("Google datastore query interpreter failed: %v", err)
		return nil, err
	}

	return resp, nil
}

func (m *GoogleDSDataSource) Table(table string) (*models.Table, error) {
	//u.Debugf("get table for %s", table)
	return m.loadTableSchema(table)
}

// interface for Upsert.Put()
func (m *GoogleDSDataSource) Put(ctx context.Context, key datasource.Key, row interface{}) (datasource.Key, error) {
	// TODO:
	if key == nil {
		return nil, fmt.Errorf("Must have key for inserts in DataStore")
	}
	return nil, nil
}
func (m *GoogleDSDataSource) PutMulti(ctx context.Context, keys []datasource.Key, src interface{}) ([]datasource.Key, error) {
	return nil, fmt.Errorf("not implemented")
}

// Interface for Deletion
func (m *GoogleDSDataSource) Delete(key driver.Value) (int, error) {
	return 0, fmt.Errorf("not implemented")
}
func (m *GoogleDSDataSource) DeleteExpression(where expr.Node) (int, error) {
	/*
		evaluator := vm.Evaluator(where)
		deletedKeys := make([]driver.Value, 0)
		for idx, row := range m.data {
			msgCtx := datasource.NewSqlDriverMessageMapVals(uint64(idx), row, m.cols)
			whereValue, ok := evaluator(msgCtx)
			if !ok {
				u.Debugf("could not evaluate where: %v   %v", idx, msgCtx.Values())
				//return deletedCt, fmt.Errorf("Could not evaluate where clause")
				continue
			}
			switch whereVal := whereValue.(type) {
			case value.BoolValue:
				if whereVal.Val() == false {
					//this means do NOT delete
				} else {
					// Delete!
					indexVal := row[m.indexCol]
					deletedKeys = append(deletedKeys, indexVal)
				}
			case nil:
				// ??
			default:
				if whereVal.Nil() {
					// Doesn't match, so don't delete
				} else {
					u.Warnf("unknown type? %T", whereVal)
				}
			}
		}
		for _, deleteKey := range deletedKeys {
			//u.Debugf("calling delete: %v", deleteKey)
			if ct, err := m.Delete(deleteKey); err != nil {
				u.Errorf("Could not delete key: %v", deleteKey)
			} else if ct != 1 {
				u.Errorf("delete should have removed 1 key %v", deleteKey)
			}
		}
		return len(deletedKeys), nil
	*/
	return 0, fmt.Errorf("not implemented")
}

func (m *GoogleDSDataSource) loadDatabases() error {

	dbs := make([]string, 0)
	sort.Strings(dbs)
	m.databases = dbs
	u.Debugf("found database names: %v", m.databases)
	found := false
	for _, db := range dbs {
		if strings.ToLower(db) == strings.ToLower(m.schema.Db) {
			found = true
		}
	}
	if !found {
		u.Warnf("could not find database: %v", m.schema.Db)
		return fmt.Errorf("Could not find that database: %v", m.schema.Db)
	}

	return nil
}

// Load only table/collection names, not full schema
func (m *GoogleDSDataSource) loadTableNames() error {

	tables := make([]string, 0)
	rows := pageQuery(datastore.NewQuery("__kind__").Run(m.dsCtx))
	for _, row := range rows {
		if !strings.HasPrefix(row.key.Name(), "__") {
			//u.Warnf("%#v", row.key)
			tables = append(tables, row.key.Name())
			m.loadTableSchema(row.key.Name())
		}

	}
	sort.Strings(tables)
	m.schema.TableNames = tables
	//u.Debugf("found tables: %v", m.schema.TableNames)
	return nil
}

func titleCase(table string) string {
	table = strings.ToLower(table)
	return strings.ToUpper(table[0:1]) + table[1:]
}

func (m *GoogleDSDataSource) loadTableSchema(table string) (*models.Table, error) {

	//u.LogTracef(u.WARN, "hello %q", table)
	if m.schema == nil {
		return nil, fmt.Errorf("no schema in use")
	}
	// check cache first
	tableLower := strings.ToLower(table)
	if tbl, ok := m.schema.Tables[tableLower]; ok && tbl.Current() {
		return tbl, nil
	}

	/*
		- Datastore keeps list of all indexed properties available
		- then we will need to ?? sample some others?
		TODO:
			- Need to recurse through enough records to get good idea of types
	*/
	tbl := models.NewTable(table, m.schema)

	// We are going to scan this table, introspecting a few rows
	// to see what types they might be
	props := pageQuery(datastore.NewQuery(table).Limit(20).Run(m.dsCtx))
	for _, row := range props {

		for _, p := range row.props {
			//u.Warnf("%#v ", p)
			colName := strings.ToLower(p.Name)

			if tbl.HasField(colName) {
				continue
			}
			//u.Debugf("%d found col: %s %T=%v", i, colName, p.Value, p.Value)
			switch val := p.Value.(type) {
			case *datastore.Key:
				//u.Debugf("found datastore.Key: %v='%#v'", colName, val)
				tbl.AddField(models.NewField(p.Name, value.StringType, 24, "Key"))
				tbl.AddValues([]driver.Value{p.Name, "string", "NO", "PRI", "Key", ""})
			case string:
				//u.Debugf("found property.Value string: %v='%#v'", colName, val)
				tbl.AddField(models.NewField(colName, value.StringType, 32, "string"))
				tbl.AddValues([]driver.Value{colName, "string", "NO", "", "", "string"})
			case int:
				//u.Debugf("found int: %v='%v'", colName, val)
				tbl.AddField(models.NewField(colName, value.IntType, 32, "int"))
				tbl.AddValues([]driver.Value{colName, "int", "NO", "", "", "int"})
			case int64:
				//u.Debugf("found int64: %v='%v'", colName, val)
				tbl.AddField(models.NewField(colName, value.IntType, 32, "long"))
				tbl.AddValues([]driver.Value{colName, "long", "NO", "", "", "long"})
			case float64:
				//u.Debugf("found float64: %v='%v'", colName, val)
				tbl.AddField(models.NewField(colName, value.NumberType, 32, "float64"))
				tbl.AddValues([]driver.Value{colName, "float64", "NO", "", "", "float64"})
			case bool:
				//u.Debugf("found string: %v='%v'", colName, val)
				tbl.AddField(models.NewField(colName, value.BoolType, 1, "bool"))
				tbl.AddValues([]driver.Value{colName, "bool", "NO", "", "", "bool"})
			case time.Time:
				//u.Debugf("found time.Time: %v='%v'", colName, val)
				tbl.AddField(models.NewField(colName, value.TimeType, 32, "datetime"))
				tbl.AddValues([]driver.Value{colName, "datetime", "NO", "", "", "datetime"})
			// case *time.Time: // datastore doesn't allow pointers
			// 	//u.Debugf("found time.Time: %v='%v'", colName, val)
			// 	tbl.AddField(models.NewField(colName, value.TimeType, 32, "datetime"))
			// 	tbl.AddValues([]driver.Value{colName, "datetime", "NO", "", "", "datetime"})
			case []uint8:
				tbl.AddField(models.NewField(colName, value.ByteSliceType, 256, "[]byte"))
				tbl.AddValues([]driver.Value{colName, "binary", "NO", "", "", "[]byte"})
			default:
				u.Warnf("%T  %#v", val, p)
			}
		}
	}
	if len(tbl.FieldMap) > 0 {
		//u.Infof("caching schem:%p   %q", m.schema, tableLower)
		m.schema.Tables[tableLower] = tbl
		return tbl, nil
	}

	return nil, fmt.Errorf("not found")
}

func discoverType(iVal interface{}) value.ValueType {

	switch iVal.(type) {
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

func pageQuery(iter *datastore.Iterator) []schemaType {
	rows := make([]schemaType, 0)
	for {
		row := schemaType{}
		if key, err := iter.Next(&row); err != nil {
			if err == datastore.Done {
				break
			}
			u.Errorf("error: %v", err)
			break
		} else {
			row.key = key
			//u.Debugf("key:  %#v", key)
			rows = append(rows, row)
		}
	}
	return rows
}

type schemaType struct {
	Vals  map[string]interface{}
	props []datastore.Property
	key   *datastore.Key
}

func (m *schemaType) Load(props []datastore.Property) error {
	m.Vals = make(map[string]interface{}, len(props))
	m.props = props
	//u.Infof("Load: %#v", props)
	for _, p := range props {
		//u.Infof("prop: %#v", p)
		m.Vals[p.Name] = p.Value
	}
	return nil
}
func (m *schemaType) Save() ([]datastore.Property, error) {
	return nil, nil
}
