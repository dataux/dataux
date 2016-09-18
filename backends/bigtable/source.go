package bigtable

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	u "github.com/araddon/gou"

	"cloud.google.com/go/bigtable"
	"golang.org/x/net/context"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/rel"
	"github.com/araddon/qlbridge/schema"
	"github.com/araddon/qlbridge/value"
)

const (
	DataSourceLabel = "bigtable"
)

var (
	ErrNoSchema = fmt.Errorf("No schema or configuration exists")

	SchemaRefreshInterval = time.Duration(time.Minute * 5)

	// Ensure our Google BigTable implements schema.Source interface
	_ schema.Source = (*Source)(nil)

	// BigTable client stuff
	client              *bigtable.Client
	adminClient         *bigtable.AdminClient
	instanceAdminClient *bigtable.InstanceAdminClient
)

func init() {
	// We need to register our DataSource provider here
	datasource.Register(DataSourceLabel, &Source{})
}

func getClient(project, instance string) (*bigtable.Client, error) {
	if client == nil {
		var err error
		client, err = bigtable.NewClient(context.Background(), project, instance)
		return client, err
	}
	return client, nil
}

func getAdminClient(project, instance string) (*bigtable.AdminClient, error) {
	if adminClient == nil {
		var err error
		adminClient, err = bigtable.NewAdminClient(context.Background(), project, instance)
		return adminClient, err
	}
	return adminClient, nil
}

// Source is a BigTable datasource, this provides Reads, Insert, Update, Delete
// - singleton shared instance
// - creates clients to bigtable (clients perform queries)
// - provides schema info about bigtable table/column-families
type Source struct {
	db               string
	project          string
	instance         string
	tables           []string // Lower cased
	tablemap         map[string]*schema.Table
	conf             *schema.ConfigSource
	schema           *schema.SchemaSource
	client           *bigtable.Client
	ac               *bigtable.AdminClient
	lastSchemaUpdate time.Time
	mu               sync.Mutex
	closed           bool
}

// Mutator a bigtable mutator connection
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

	m.instance = m.conf.Settings.String("instance")
	if len(m.instance) == 0 {
		return fmt.Errorf("No 'instance' for bigtable found in config %v", m.conf.Settings)
	}

	m.project = m.conf.Settings.String("project")
	if len(m.project) == 0 {
		return fmt.Errorf("No 'project' for bigtable found in config %v", m.conf.Settings)
	}

	client, err := getClient(m.project, m.instance)
	if err != nil {
		u.Errorf("Could not create bigtable client %v", err)
		return err
	}
	m.client = client

	ac, err := getAdminClient(m.project, m.instance)
	if err != nil {
		u.Errorf("Could not create bigtable adminclient %v", err)
		return err
	}
	m.ac = ac

	m.loadSchema()
	return nil
}

func (m *Source) loadSchema() error {

	ctx := context.Background()
	tables, err := m.ac.Tables(ctx)
	if err != nil {
		u.Errorf("Getting list of tables: %v", err)
		return err
	}
	sort.Strings(tables)
	for _, table := range tables {
		ti, err := m.ac.TableInfo(ctx, table)
		if err != nil {
			u.Errorf("Getting table info: %v", err)
			return err
		}
		sort.Strings(ti.Families)
		u.Debugf("Found Table: %v", table, ti.Families)
	}

	m.lastSchemaUpdate = time.Now()
	m.tables = make([]string, 0)

	for _, table := range tables {
		tbl := schema.NewTable(strings.ToLower(table))
		//u.Infof("building tbl schema %v", table)
		colNames := make([]string, 0)

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
