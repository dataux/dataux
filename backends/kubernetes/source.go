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
)

const (
	DataSourceLabel = "kubernetes"
)

var (
	ErrNoSchema = fmt.Errorf("No schema or configuration exists")

	endpoints = []string{"pods", "nodes"}

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
// - provides schema info about rest apis
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

	m.cluster = m.conf.Settings.String("cluster")

	// uses the current context in kubeconfig
	config, err := clientcmd.BuildConfigFromFlags("", os.Getenv("HOME")+"/.kube/config")
	if err != nil {
		u.Errorf("could not read kube config %v", err)
		return err
	}
	m.kconfig = config

	m.loadSchema()
	return nil
}

func (m *Source) loadSchema() error {

	var tablesToLoad map[string]struct{}

	if len(m.schema.Conf.TablesToLoad) > 0 {
		tablesToLoad = make(map[string]struct{}, len(m.schema.Conf.TablesToLoad))
		for _, tableToLoad := range m.schema.Conf.TablesToLoad {
			tablesToLoad[tableToLoad] = struct{}{}
		}
	}

	m.lastSchemaUpdate = time.Now()
	m.tables = make([]string, 0)

	// creates the client
	clientset, err := kubernetes.NewForConfig(m.kconfig)
	if err != nil {
		u.Errorf("could not connect to kubernetes %v", err)
		return err
	}

	pods, err := clientset.Core().Pods("").List(api.ListOptions{})
	if err != nil {
		panic(err.Error())
	}
	u.Infof("There are %d pods in the cluster\n", len(pods.Items))
	for _, pod := range pods.Items {
		podJson, _ := json.MarshalIndent(pod, "", "  ")
		u.Debugf("\n%s", string(podJson))
	}

	for _, table := range endpoints {

		tbl := schema.NewTable(strings.ToLower(table))

		//tbl.AddContext("bigtable_table", btt)
		//u.Infof("%p  caching table %q  cols=%v", m.schema, tbl.Name, colNames)
		tbl.SetColumns(nil)
		m.tablemap[tbl.Name] = tbl
		m.tables = append(m.tables, tbl.Name)
	}

	sort.Strings(m.tables)
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

	return nil, schema.ErrNotFound
}

func (m *Source) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.closed = true
	return nil
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

	return NewSqlToKube(m, tbl), nil
}
