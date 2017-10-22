package bigtable

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	u "github.com/araddon/gou"

	"cloud.google.com/go/bigtable"
	"golang.org/x/net/context"

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

	gceProject = os.Getenv("GCEPROJECT")
)

func init() {
	// We need to register our DataSource provider here
	schema.RegisterSourceType(DataSourceLabel, &Source{})
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
	schema           *schema.Schema
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

func (m *Source) Init() {}

func (m *Source) Setup(ss *schema.Schema) error {

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
		if gceProject != "" {
			m.project = gceProject
		} else {
			return fmt.Errorf("No 'project' for bigtable found in config %v", m.conf.Settings)
		}
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

type bttable struct {
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

	ctx := context.Background()
	tableNames, err := m.ac.Tables(ctx)
	if err != nil {
		u.Errorf("BigTable get list of tables err:%v", err)
		return err
	}
	sort.Strings(tableNames)
	tables := make([]*bttable, 0, len(tableNames))
	for _, table := range tableNames {

		if len(tablesToLoad) > 0 {
			if _, shouldLoad := tablesToLoad[table]; !shouldLoad {
				continue
			}
		}

		ti, err := m.ac.TableInfo(ctx, table)
		if err != nil {
			u.Errorf("Getting table info: %v", err)
			return err
		}
		sort.Strings(ti.Families)
		tables = append(tables, &bttable{table, ti.Families})
		u.Debugf("Found Table: %v families:%v", table, ti.Families)
	}

	m.lastSchemaUpdate = time.Now()
	m.tables = make([]string, 0)
	//colFamilies := make(map[string]string)

	for _, table := range tables {

		btt := m.client.Open(table.Name)

		for _, colFamily := range table.Families {

			// We are currently treating col-families as "table"
			// we should allow control/customization of this
			tbl := schema.NewTable(strings.ToLower(colFamily))
			tbl.Parent = table.Name
			//u.Infof("building tbl schema %v", colFamily)
			colNames := make([]string, 0)
			colMap := make(map[string]bool)
			colFamilyPrefix := colFamily + ":"

			var rr bigtable.RowRange
			//rr = bigtable.FamilyFilter(colFamily)
			// if start, end := parsed["start"], parsed["end"]; end != "" {
			// 	rr = bigtable.NewRange(start, end)
			// } else if start != "" {
			// 	rr = bigtable.InfiniteRange(start)
			// }
			// if prefix := parsed["prefix"]; prefix != "" {
			// 	rr = bigtable.PrefixRange(prefix)
			// }

			var opts []bigtable.ReadOption
			opts = append(opts, bigtable.LimitRows(20))
			opts = append(opts, bigtable.RowFilter(bigtable.FamilyFilter(colFamily)))
			opts = append(opts, bigtable.RowFilter(bigtable.LatestNFilter(1)))

			err := btt.ReadRows(ctx, rr, func(r bigtable.Row) bool {

				ris := r[colFamily]
				sort.Sort(byColumn(ris))
				//u.Debugf("tbl:%s cf:%s  key: %v  cellct:%d", table.Name, colFamily, r.Key(), len(ris))
				for _, ri := range ris {
					//ts := time.Unix(0, int64(ri.Timestamp)*1e3)
					//u.Debugf("%-20s  %-40s @ %v  %q", colFamily, ri.Column, ts, ri.Value)

					colName := strings.ToLower(ri.Column)
					colName = strings.Replace(colName, colFamilyPrefix, "", 1)
					if _, exists := colMap[colName]; exists {
						continue
					}
					colMap[colName] = true
					colNames = append(colNames, colName)

					var f *schema.Field
					vt := value.ValueTypeFromStringAll(string(ri.Value))
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
						u.Warnf("unknown column type %#v", string(ri.Value))
						continue
					}
					//u.Debugf("%s = %v vt:%s  %#v", colName, string(ri.Value), vt, f)
					tbl.AddField(f)
				}

				return true
			}, opts...)
			if err != nil {
				// retry
				return err
			}

			//tbl.AddContext("bigtable_table", btt)
			//u.Infof("%p  caching table %q  cols=%v", m.schema, tbl.Name, colNames)
			tbl.SetColumns(colNames)
			m.tablemap[tbl.Name] = tbl
			m.tables = append(m.tables, tbl.Name)
		}
	}
	sort.Strings(m.tables)
	return nil
}

type byColumn []bigtable.ReadItem

func (b byColumn) Len() int           { return len(b) }
func (b byColumn) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
func (b byColumn) Less(i, j int) bool { return b[i].Column < b[j].Column }

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

	return NewSqlToBT(m, tbl), nil
}
