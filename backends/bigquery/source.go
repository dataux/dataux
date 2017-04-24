package bigquery

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	u "github.com/araddon/gou"

	"cloud.google.com/go/bigquery"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/rel"
	"github.com/araddon/qlbridge/schema"
	"github.com/araddon/qlbridge/value"
)

const (
	DataSourceLabel = "bigquery"
)

var (
	ErrNoSchema = fmt.Errorf("No schema or configuration exists")

	SchemaRefreshInterval = time.Duration(time.Minute * 5)

	// Ensure our Google BigQuery implements schema.Source interface
	_ schema.Source = (*Source)(nil)

	gceProject = os.Getenv("GCEPROJECT")
)

func init() {
	// We need to register our DataSource provider here
	datasource.Register(DataSourceLabel, &Source{})
}

// Source is a BigQuery datasource, this provides Reads, Insert, Update, Delete
// - singleton shared instance
// - creates clients to bigquery (clients perform queries)
// - provides schema info about bigtable table/column-families
type Source struct {
	db               string
	project          string
	dataset          string
	tables           []string // Lower cased
	tablemap         map[string]*schema.Table
	conf             *schema.ConfigSource
	schema           *schema.SchemaSource
	client           *bigquery.Client
	lastSchemaUpdate time.Time
	mu               sync.Mutex
	closed           bool
}

// Mutator a bigquery mutator connection
type Mutator struct {
	tbl *schema.Table
	sql rel.SqlStatement
	ds  *Source
}

func (m *Source) Init() {}

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

	m.project = m.conf.Settings.String("project")
	if len(m.project) == 0 {
		if gceProject != "" {
			m.project = gceProject
		} else {
			return fmt.Errorf("No 'project' for bigquery found in config %v", m.conf.Settings)
		}
	}

	m.dataset = m.conf.Settings.String("dataset")
	if len(m.dataset) == 0 {
		return fmt.Errorf("No 'dataset' for bigquery found in config %v", m.conf.Settings)
	}

	client, err := bigquery.NewClient(context.Background(), m.project)
	if err != nil {
		u.Warnf("Could not create bigquery client %v", err)
		return err
	}
	m.client = client

	m.loadSchema()
	return nil
}

type qttable struct {
	Name     string
	Families []string
}

func (m *Source) loadSchema() error {

	var tablesToLoad map[string]struct{}

	if len(m.schema.Conf.TablesToLoad) > 0 {
		tablesToLoad = make(map[string]struct{}, len(m.schema.Conf.TablesToLoad))
		for _, tableToLoad := range m.schema.Conf.TablesToLoad {
			tablesToLoad[tableToLoad] = struct{}{}
		}
	}

	tableNames := make([]string, 0)

	ctx := context.Background()

	bqds := m.client.Dataset(m.dataset)

	tbliter := bqds.Tables(ctx)

	for {
		t, err := tbliter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			u.Warnf("could not read tables %v", err)
			return err
		}
		u.Debugf("table fqname:%s  id:%s", t.FullyQualifiedName(), t.TableID)
		table := t.TableID
		if len(tablesToLoad) > 0 {
			if _, shouldLoad := tablesToLoad[table]; !shouldLoad {
				continue
			}
		}

		md, err := t.Metadata(ctx)
		if err != nil {
			u.Warnf("could not read tables %v", err)
			return err
		}
		u.Debugf("%#v", md)

		tableNames = append(tableNames, table)

		tbl := schema.NewTable(strings.ToLower(table))
		tbl.Parent = t.DatasetID
		//u.Infof("building tbl schema %v", colFamily)
		colNames := make([]string, 0)

		//u.Debugf("tbl:%s cf:%s  key: %v  cellct:%d", table.Name, colFamily, r.Key(), len(ris))
		for _, fs := range md.Schema {
			//ts := time.Unix(0, int64(ri.Timestamp)*1e3)
			u.Debugf("%#v", fs)
			colName := strings.ToLower(fs.Name)

			var f *schema.Field
			vt := value.ValueTypeFromStringAll("fake")
			switch vt {
			case value.JsonType:
				f = schema.NewFieldBase(colName, value.JsonType, 2000, "json")
			case value.IntType:
				f = schema.NewFieldBase(colName, value.IntType, 32, "int")
			case value.NumberType:
				f = schema.NewFieldBase(colName, value.NumberType, 64, "float64")
			case value.BoolType:
				f = schema.NewFieldBase(colName, value.BoolType, 1, "bool")
			case value.TimeType:
				f = schema.NewFieldBase(colName, value.TimeType, 64, "datetime")
			case value.StringType:
				f = schema.NewFieldBase(colName, value.StringType, 200, "varchar")
			default:
				u.Warnf("unknown column type %#v", fs)
				continue
			}
			//u.Debugf("%s = %v vt:%s  %#v", colName, string(ri.Value), vt, f)
			tbl.AddField(f)
		}

		//tbl.AddContext("bigtable_table", btt)
		//u.Infof("%p  caching table %q  cols=%v", m.schema, tbl.Name, colNames)
		tbl.SetColumns(colNames)
		m.tablemap[tbl.Name] = tbl
	}

	sort.Strings(tableNames)

	m.tables = tableNames

	m.lastSchemaUpdate = time.Now()

	sort.Strings(m.tables)
	return nil
}

func (m *Source) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.closed = true
	return nil
}

func (m *Source) DataSource() schema.Source { return m }
func (m *Source) Tables() []string          { return m.tables }
func (m *Source) Table(table string) (*schema.Table, error) {

	//u.Debugf("Table(%q)", table)
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
	u.Debugf("Open(%v)", tableName)
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

	return NewSqlToBQ(m, tbl), nil
}
