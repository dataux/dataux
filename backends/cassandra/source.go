package cassandra

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	u "github.com/araddon/gou"
	"github.com/gocql/gocql"
	"github.com/hailocab/go-hostpool"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/rel"
	"github.com/araddon/qlbridge/schema"
	"github.com/araddon/qlbridge/value"
)

const (
	DataSourceLabel = "cassandra"
)

var (
	ErrNoSchema = fmt.Errorf("No schema or configuration exists")

	SchemaRefreshInterval = time.Duration(time.Minute * 5)

	// Ensure our Google DataStore implements datasource.DataSource interface
	_ schema.Source = (*Source)(nil)
)

func init() {
	// We need to register our DataSource provider here
	datasource.Register(DataSourceLabel, &Source{})
}

// Create a gocql session
func createCassSession(conf *schema.ConfigSource, keyspace string) (*gocql.Session, error) {

	servers := conf.Settings.Strings("hosts")
	if len(servers) == 0 {
		return nil, fmt.Errorf("No 'hosts' for cassandra found in config %v", conf.Settings)
	}
	cluster := gocql.NewCluster(servers...)

	// Error querying table schema: Undefined name key_aliases in selection clause
	//  if on cass 2.1 use 3.0.0 for cqlversion
	cluster.ProtoVersion = 4
	cluster.CQLVersion = "3.1.0"

	cluster.Keyspace = keyspace
	cluster.NumConns = 10

	if numconns := conf.Settings.Int("numconns"); numconns > 0 {
		cluster.NumConns = numconns
	}

	cluster.Timeout = time.Second * 10
	cluster.PoolConfig.HostSelectionPolicy = gocql.HostPoolHostPolicy(
		hostpool.NewEpsilonGreedy(nil, 0, &hostpool.LinearEpsilonValueCalculator{}),
	)

	// load-balancers often kill idel conns so lets heart beat to keep alive
	// see https://cloud.google.com/compute/docs/troubleshooting#communicatewithinternet
	cluster.SocketKeepalive = time.Duration(5 * time.Minute)

	if retries := conf.Settings.Int("retries"); retries > 0 {
		cluster.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: retries}
	} else {
		cluster.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: 3}
	}

	sess, err := cluster.CreateSession()
	if err != nil && strings.Contains(err.Error(), "Invalid or unsupported protocol version: 4") {
		// cass < 2.2 ie 2.1, 2.0
		cluster.ProtoVersion = 2
		cluster.CQLVersion = "3.0.0"
		sess, err = cluster.CreateSession()
	}
	return sess, err
}

// Source is a Cassandra datasource, this provides Reads, Insert, Update, Delete
// - singleton shared instance
// - creates connections to cassandra (connections perform queries)
// - provides schema info about cassandra keyspace
type Source struct {
	db               string
	keyspace         string
	kmd              *gocql.KeyspaceMetadata
	tables           []string // Lower cased
	tablemap         map[string]*schema.Table
	conf             *schema.ConfigSource
	schema           *schema.SchemaSource
	session          *gocql.Session
	lastSchemaUpdate time.Time
	mu               sync.Mutex
	closed           bool
}

// Mutator a cassandra mutator connection
type Mutator struct {
	tbl *schema.Table
	sql rel.SqlStatement
	ds  *Source
}

func (m *Source) Setup(ss *schema.SchemaSource) error {

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.schema != nil {
		return nil
	}

	m.schema = ss
	m.conf = ss.Conf
	m.db = strings.ToLower(ss.Name)
	m.tablemap = make(map[string]*schema.Table)

	//u.Infof("Init:  %#v", m.schema.Conf)
	if m.schema.Conf == nil {
		return fmt.Errorf("Schema conf not found")
	}

	m.keyspace = m.conf.Settings.String("keyspace")
	if len(m.keyspace) == 0 {
		//return fmt.Errorf("No 'keyspace' for cassandra found in config %v", m.conf.Settings)
		m.keyspace = m.db
	}

	sess, err := createCassSession(m.conf, m.keyspace)
	if err != nil {
		u.Errorf("Could not create cass conn %v", err)
		return err
	}
	m.session = sess

	m.loadSchema()
	return nil
}

func (m *Source) loadSchema() error {

	kmd, err := m.session.KeyspaceMetadata(m.keyspace)
	if err != nil {
		u.Warnf("ks:%v  change protocol version if 2.1 or earlier %v", m.keyspace, err)
		return err
	}
	m.kmd = kmd
	m.lastSchemaUpdate = time.Now()
	m.tables = make([]string, 0)

	for _, cf := range kmd.Tables {
		tbl := schema.NewTable(strings.ToLower(cf.Name))
		//u.Infof("building tbl schema %v", cf.Name)
		colNames := make([]string, 0)
		/*
			col &gocql.ColumnMetadata{Keyspace:"datauxtest", Table:"article", Name:"author", ComponentIndex:0, Kind:"partition_key",
				Validator:"org.apache.cassandra.db.marshal.UTF8Type", Type:gocql.NativeType{proto:0x0, typ:13, custom:""},
				ClusteringOrder:"", Order:false, Index:gocql.ColumnIndexMetadata{Name:"", Type:"", Options:map[string]interface {}(nil)}}
			col &gocql.ColumnMetadata{Keyspace:"datauxtest", Table:"article", Name:"body", ComponentIndex:0, Kind:"regular",
				Validator:"org.apache.cassandra.db.marshal.BytesType", Type:gocql.NativeType{proto:0x0, typ:3, custom:""},
				ClusteringOrder:"", Order:false, Index:gocql.ColumnIndexMetadata{Name:"", Type:"", Options:map[string]interface {}(nil)}}
			col &gocql.ColumnMetadata{Keyspace:"datauxtest", Table:"article", Name:"category", ComponentIndex:0, Kind:"regular",
				Validator:"org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.UTF8Type)",
				Type:gocql.CollectionType{NativeType:gocql.NativeType{proto:0x0, typ:34, custom:""}, Key:gocql.TypeInfo(nil),
				Elem:gocql.NativeType{proto:0x0, typ:13, custom:""}}, ClusteringOrder:"", Order:false,
				Index:gocql.ColumnIndexMetadata{Name:"article_category_idx", Type:"COMPOSITES", Options:map[string]interface {}(nil)}}

		*/
		for _, col := range cf.Columns {
			colName := strings.ToLower(col.Name)
			colNames = append(colNames, colName)

			//u.Debugf("%-20s %-12s %-20s %d %-12s", colName, col.Type.Type(), col.Kind, col.ComponentIndex, col.ClusteringOrder)
			var f *schema.Field
			switch col.Type.Type() {
			case gocql.TypeBlob:
				f = schema.NewFieldBase(colName, value.ByteSliceType, 2000, "blob")
			case gocql.TypeVarchar:
				f = schema.NewFieldBase(colName, value.StringType, 256, "string")
			case gocql.TypeText:
				f = schema.NewFieldBase(colName, value.StringType, 2000, "string")
			case gocql.TypeInt, gocql.TypeTinyInt:
				f = schema.NewFieldBase(colName, value.IntType, 32, "int")
			case gocql.TypeBigInt:
				f = schema.NewFieldBase(colName, value.IntType, 64, "long")
			case gocql.TypeFloat, gocql.TypeDouble, gocql.TypeDecimal:
				f = schema.NewFieldBase(colName, value.NumberType, 64, "float64")
			case gocql.TypeBoolean:
				f = schema.NewFieldBase(colName, value.BoolType, 1, "bool")
			case gocql.TypeDate, gocql.TypeTimestamp:
				f = schema.NewFieldBase(colName, value.TimeType, 64, "datetime")
			case gocql.TypeSet:
				switch nt := col.Type.(type) {
				case gocql.CollectionType:
					//u.Warnf("SET TYPE CASSANDRA Not handled very well?!  \n%v  \n%#v \n%#v", nt.Type(), nt, col)
					switch nt.Elem.Type() {
					case gocql.TypeText, gocql.TypeVarchar:
						f = schema.NewFieldBase(colName, value.StringsType, 256, "[]string")
					case gocql.TypeInt, gocql.TypeBigInt, gocql.TypeTinyInt:
						f = schema.NewFieldBase(colName, value.SliceValueType, 256, "[]int")
						f.NativeType = value.IntType
					default:
						u.Warnf("SET TYPE CASSANDRA Not handled very well?!  %v  \n%v", nt.Type(), nt.NativeType.Type())
					}
				}
				/*
					switch col.Type.(type) {
					case gocql.TypeVarchar, gocql.TypeText:
						f = schema.NewFieldBase(colName, value.StringsType, 256, "[]string")
					case gocql.TypeInt, gocql.TypeBigInt, gocql.TypeTinyInt:
						f = schema.NewFieldBase(colName, value.SliceValueType, 256, "[]int")
						f.NativeType = value.IntType
					default:
						u.Warnf("SET TYPE CASSANDRA Not handled very well?!  %#v  \n%#v", col.Type, col)
					}
				*/

			case gocql.TypeMap:

				switch col.Type.Type() {
				case gocql.TypeVarchar, gocql.TypeText:
					f = schema.NewFieldBase(colName, value.MapStringType, 256, "map[string]string")
					f.NativeType = value.MapStringType
				case gocql.TypeInt, gocql.TypeBigInt, gocql.TypeTinyInt:
					f = schema.NewFieldBase(colName, value.MapIntType, 256, "map[string]string")
					f.NativeType = value.MapStringType
				case gocql.TypeTimestamp, gocql.TypeTime, gocql.TypeDate:
					f = schema.NewFieldBase(colName, value.MapTimeType, 256, "map[string]time")
					f.NativeType = value.MapTimeType
				}
				u.Warnf("MAP TYPE CASSANDRA Not handled very well?!")
			default:
				u.Warnf("unknown column type %#v", col)
			}
			if f != nil {
				// Lets save the Cass Column Metadata for later usage
				f.AddContext("cass_column", col)
				tbl.AddField(f)
				//u.Debugf("col %+v    %#v", f, col)
			}

		}

		tbl.AddContext("cass_table", cf)
		//u.Infof("%p  caching table %q  cols=%v", m.schema, tbl.Name, colNames)
		tbl.SetColumns(colNames)
		m.tablemap[tbl.Name] = tbl
		m.tables = append(m.tables, tbl.Name)
	}
	sort.Strings(m.tables)
	return nil
}

func (m *Source) Close() error {
	u.Infof("Closing Cassandra Source %p", m)
	m.mu.Lock()
	defer m.mu.Unlock()

	m.closed = true
	return nil
}

func (m *Source) DataSource() schema.Source { return m }
func (m *Source) Tables() []string          { return m.tables }
func (m *Source) Table(table string) (*schema.Table, error) {

	if m.schema == nil {
		u.Warnf("no schema in use?")
		return nil, fmt.Errorf("no schema in use")
	}

	table = strings.ToLower(table)
	tbl := m.tablemap[table]
	if tbl != nil {
		return tbl, nil
	}

	if m.lastSchemaUpdate.After(time.Now().Add(SchemaRefreshInterval)) {
		u.Warnf("that table %q does not exist in this schema, refreshing")
		m.loadSchema()
		return m.Table(table)
	}
	return nil, schema.ErrNotFound
}

func (m *Source) Open(tableName string) (schema.Conn, error) {
	//u.Debugf("Open(%v)", tableName)
	if m.schema == nil {
		u.Warnf("no schema?")
		return nil, nil
	}
	tableName = strings.ToLower(tableName)
	tbl, err := m.schema.Table(tableName)
	if err != nil {
		return nil, err
	}
	if tbl == nil {
		u.Errorf("Could not find table for '%s'.'%s'", m.schema.Name, tableName)
		return nil, fmt.Errorf("Could not find '%v'.'%v' schema", m.schema.Name, tableName)
	}

	return NewSqlToCql(m, tbl), nil
}
