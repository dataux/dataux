package kubernetes

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	u "github.com/araddon/gou"

	"k8s.io/client-go/1.4/kubernetes"
	"k8s.io/client-go/1.4/pkg/api"
	"k8s.io/client-go/1.4/rest"
	"k8s.io/client-go/1.4/tools/clientcmd"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/rel"
	"github.com/araddon/qlbridge/schema"
	"github.com/araddon/qlbridge/value"
)

const (
	DataSourceLabel = "kubernetes"
)

var (
	ErrNoSchema = fmt.Errorf("No schema or configuration exists")

	endpoints = []string{"pods", "nodes", "services"}

	// Ensure our Kubernetes source implements schema.Source interface
	_ schema.Source = (*Source)(nil)
)

func init() {
	// We need to register our DataSource provider here
	datasource.Register(DataSourceLabel, &Source{})
}

// Source is a Kubernetes datasource, this provides Reads, Insert, Update, Delete
// - singleton shared instance
// - creates clients to kube api (clients perform queries)
// - provides schema info about the apis
type Source struct {
	db               string
	cluster          string
	tables           []string // Lower cased
	tablemap         map[string]*schema.Table
	conf             *schema.ConfigSource
	schema           *schema.SchemaSource
	kconfig          *rest.Config
	lastSchemaUpdate time.Time
	mu               sync.Mutex
	closed           bool
}

// Mutator a mutator connection
type Mutator struct {
	tbl *schema.Table
	sql rel.SqlStatement
	ds  *Source
}

// Setup accepts the schema source config
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

	//u.Debugf("Kube Source Init:  %#v", m.schema.Conf)
	if m.schema.Conf == nil {
		return fmt.Errorf("Schema conf not found for kubernetes")
	}

	m.cluster = m.conf.Settings.String("cluster")

	// uses the current context in kubeconfig
	// TODO:   allow this to be specified
	config, err := clientcmd.BuildConfigFromFlags("", os.Getenv("HOME")+"/.kube/config")
	if err != nil {
		u.Errorf("could not read kube config %v", err)
		return err
	}
	m.kconfig = config

	return m.loadSchema()
}

func (m *Source) loadSchema() error {

	var tablesToLoad map[string]struct{}

	// If we limit the tables to load down to subset of available
	// it will be listed here in conf
	if len(m.schema.Conf.TablesToLoad) > 0 {
		tablesToLoad = make(map[string]struct{}, len(m.schema.Conf.TablesToLoad))
		for _, tableToLoad := range m.schema.Conf.TablesToLoad {
			tablesToLoad[tableToLoad] = struct{}{}
		}
	}

	m.lastSchemaUpdate = time.Now()
	m.tables = make([]string, 0)

	// creates the kube grpc client
	clientset, err := kubernetes.NewForConfig(m.kconfig)
	if err != nil {
		u.Errorf("could not connect to kubernetes %v", err)
		return err
	}

	if err := m.describePods(clientset); err != nil {
		return err
	}

	sort.Strings(m.tables)
	return nil
}

func (m *Source) describePods(c *kubernetes.Clientset) error {

	pods, err := c.Core().Pods("").List(api.ListOptions{})
	if err != nil {
		return fmt.Errorf("Could not get kubernetes pods %v", err)
	}
	u.Debugf("describe pods: %q", pods.Kind)
	u.Infof("There are %d pods in the cluster", len(pods.Items))
	for _, pod := range pods.Items {
		podJson, _ := json.MarshalIndent(pod, "", "  ")
		if true == false {
			u.Debugf("\n%s", string(podJson))
		}
	}

	tbl := schema.NewTable("pods")
	colNames := make([]string, 0, 40)
	tbl.AddField(schema.NewFieldBase("kind", value.StringType, 24, "string"))
	colNames = append(colNames, "kind")
	colNames = m.describeMetaData(tbl, colNames)
	/*
		var f *schema.Field
		switch val := p.Value.(type) {
		case *datastore.Key:
			f = schema.NewFieldBase(p.Name, value.StringType, 24, "Key")
		case string:
			f = schema.NewFieldBase(colName, value.StringType, 256, "string")
		case int:
			f = schema.NewFieldBase(colName, value.IntType, 32, "int")
		case int64:
			f = schema.NewFieldBase(colName, value.IntType, 64, "long")
		case float64:
			f = schema.NewFieldBase(colName, value.NumberType, 64, "float64")
		case bool:
			f = schema.NewFieldBase(colName, value.BoolType, 1, "bool")
		case time.Time:
			f = schema.NewFieldBase(colName, value.TimeType, 32, "datetime")
		case []uint8:
			f = schema.NewFieldBase(colName, value.ByteSliceType, 256, "[]byte")
		default:
			u.Warnf("google datastore unknown type %T  %#v", val, p)
		}
		if f != nil {
			tbl.AddField(f)
			colNames = append(colNames, colName)
		}
	*/

	//tbl.AddContext("kube_table", btt)
	u.Infof("%p  caching table p=%p %q  cols=%v", m.schema, tbl, tbl.Name, colNames)
	tbl.SetColumns(colNames)
	m.tablemap[tbl.Name] = tbl
	m.tables = append(m.tables, tbl.Name)
	return nil
}

func (m *Source) describeMetaData(tbl *schema.Table, colNames []string) []string {

	// http://kubernetes.io/docs/api-reference/v1/definitions/#_v1_objectmeta
	tbl.AddField(schema.NewFieldBase("name", value.StringType, 256, "string"))
	tbl.AddField(schema.NewFieldBase("generatename", value.StringType, 256, "string"))
	tbl.AddField(schema.NewFieldBase("namespace", value.StringType, 256, "string"))
	tbl.AddField(schema.NewFieldBase("selflink", value.StringType, 256, "string"))
	tbl.AddField(schema.NewFieldBase("uid", value.StringType, 32, "string"))
	tbl.AddField(schema.NewFieldBase("resourceversion", value.StringType, 8, "string"))
	tbl.AddField(schema.NewFieldBase("generation", value.IntType, 64, "long"))
	tbl.AddField(schema.NewFieldBase("creationtimestamp", value.TimeType, 32, "datetime"))
	tbl.AddField(schema.NewFieldBase("deletiontimestamp", value.TimeType, 32, "datetime"))
	tbl.AddField(schema.NewFieldBase("labels", value.ByteSliceType, 256, "object"))
	colNames = append(colNames, []string{"name", "generatename", "namespace", "selflink",
		"uid", "resourceversion", "generation", "creationtimestamp", "deletiontimestamp", "labels"}...)
	return colNames
}

func (m *Source) DataSource() schema.Source { return m }
func (m *Source) Tables() []string          { return m.tables }
func (m *Source) Table(table string) (*schema.Table, error) {

	u.Debugf("Table(%q)", table)
	if m.schema == nil {
		u.Warnf("no schema in use?")
		return nil, fmt.Errorf("no schema in use")
	}

	table = strings.ToLower(table)
	tbl := m.tablemap[table]
	if tbl != nil {
		return tbl, nil
	}

	return nil, schema.ErrNotFound
}

func (m *Source) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.closed = true
	return nil
}

func (m *Source) Open(tableName string) (schema.Conn, error) {
	u.Debugf("Open(%q)", tableName)
	if m.schema == nil {
		u.Warnf("no schema for %q", tableName)
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

	return NewSqlToKube(m, tbl)
}
