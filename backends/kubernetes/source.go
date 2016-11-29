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
	//u.Debugf("Open(%q)", tableName)
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

	if err := m.describeNodes(clientset); err != nil {
		return err
	}

	if err := m.describeServices(clientset); err != nil {
		return err
	}

	sort.Strings(m.tables)
	return nil
}

func (m *Source) describeServices(c *kubernetes.Clientset) error {

	services, err := c.Core().Services("").List(api.ListOptions{})
	if err != nil {
		return fmt.Errorf("Could not get kubernetes services %v", err)
	}
	u.Debugf("describe services: %q", services.Kind)
	u.Infof("There are %d services in the cluster", len(services.Items))
	for _, svc := range services.Items {
		svcJson, _ := json.MarshalIndent(svc, "", "  ")
		if true == true {
			u.Debugf("\n%s", string(svcJson))
		}
	}

	// http://kubernetes.io/docs/api-reference/v1/definitions/#_v1_service
	tbl := schema.NewTable("services")
	colNames := make([]string, 0, 40)
	tbl.AddField(schema.NewFieldBase("kind", value.StringType, 24, "string"))
	colNames = append(colNames, "kind")
	colNames = m.describeMetaData(tbl, colNames)

	// http://kubernetes.io/docs/api-reference/v1/definitions/#_v1_servicestatus
	tbl.AddField(schema.NewFieldBase("loadbalancer", value.JsonType, 256, "json object"))
	colNames = append(colNames, []string{"loadbalancer"}...)

	// http://kubernetes.io/docs/api-reference/v1/definitions/#_v1_servicespec
	tbl.AddField(schema.NewFieldBase("ports", value.JsonType, 256, "json array"))
	tbl.AddField(schema.NewFieldBase("selector", value.JsonType, 256, "json object"))
	tbl.AddField(schema.NewFieldBase("clusterip", value.StringType, 256, "string"))
	tbl.AddField(schema.NewFieldBase("type", value.StringType, 32, "string"))
	tbl.AddField(schema.NewFieldBase("externalips", value.JsonType, 32, "json array"))
	tbl.AddField(schema.NewFieldBase("deprecatedpublicips", value.JsonType, 32, "json array"))
	tbl.AddField(schema.NewFieldBase("sessionaffinity", value.StringType, 256, "string"))
	tbl.AddField(schema.NewFieldBase("loadbalancerip", value.StringType, 256, "string"))
	tbl.AddField(schema.NewFieldBase("loadbalancersourceranges", value.JsonType, 32, "json array"))
	tbl.AddField(schema.NewFieldBase("externalname", value.StringType, 256, "string"))
	colNames = append(colNames, []string{"ports", "selector", "clusterip", "type",
		"externalips", "deprecatedpublicips", "sessionaffinity", "loadbalancerip",
		"loadbalancersourceranges", "externalname"}...)

	u.Infof("%p  caching table p=%p %q  cols=%v", m.schema, tbl, tbl.Name, colNames)
	tbl.SetColumns(colNames)
	m.tablemap[tbl.Name] = tbl
	m.tables = append(m.tables, tbl.Name)
	return nil
}

func (m *Source) describeNodes(c *kubernetes.Clientset) error {

	nodes, err := c.Core().Nodes().List(api.ListOptions{})
	if err != nil {
		return fmt.Errorf("Could not get kubernetes nodes %v", err)
	}
	u.Debugf("describe nodes: %q", nodes.Kind)
	u.Infof("There are %d nodes in the cluster", len(nodes.Items))
	for _, node := range nodes.Items {
		nodeJson, _ := json.MarshalIndent(node, "", "  ")
		if true == true {
			u.Debugf("\n%s", string(nodeJson))
		}
	}

	tbl := schema.NewTable("nodes")
	colNames := make([]string, 0, 40)
	tbl.AddField(schema.NewFieldBase("kind", value.StringType, 24, "string"))
	colNames = append(colNames, "kind")
	colNames = m.describeMetaData(tbl, colNames)

	// http://kubernetes.io/docs/api-reference/v1/definitions/#_v1_nodestatus
	tbl.AddField(schema.NewFieldBase("capacity", value.JsonType, 256, "json object"))
	tbl.AddField(schema.NewFieldBase("allocatable", value.JsonType, 256, "json object"))
	tbl.AddField(schema.NewFieldBase("phase", value.StringType, 256, "string"))
	tbl.AddField(schema.NewFieldBase("conditions", value.JsonType, 256, "json array"))
	tbl.AddField(schema.NewFieldBase("addresses", value.JsonType, 256, "json array"))
	tbl.AddField(schema.NewFieldBase("daemonendpoints", value.JsonType, 256, "json object"))
	tbl.AddField(schema.NewFieldBase("nodeinfo", value.JsonType, 256, "json object"))
	tbl.AddField(schema.NewFieldBase("images", value.JsonType, 256, "json array"))
	tbl.AddField(schema.NewFieldBase("volumesinuse", value.JsonType, 256, "json array"))
	tbl.AddField(schema.NewFieldBase("volumesattached", value.JsonType, 256, "json array"))
	colNames = append(colNames, []string{"capacity", "allocatable", "phase", "conditions",
		"addresses", "daemonendpoints", "nodeinfo", "images", "volumesinuse", "volumesattached"}...)

	// http://kubernetes.io/docs/api-reference/v1/definitions/#_v1_nodespec
	tbl.AddField(schema.NewFieldBase("podcidr", value.StringType, 256, "string"))
	tbl.AddField(schema.NewFieldBase("externalid", value.StringType, 32, "string"))
	tbl.AddField(schema.NewFieldBase("providerid", value.StringType, 32, "string"))
	tbl.AddField(schema.NewFieldBase("unschedulable", value.BoolType, 1, "boolean"))

	colNames = append(colNames, []string{"podcidr", "externalid", "providerid", "unschedulable"}...)

	u.Infof("%p  caching table p=%p %q  cols=%v", m.schema, tbl, tbl.Name, colNames)
	tbl.SetColumns(colNames)
	m.tablemap[tbl.Name] = tbl
	m.tables = append(m.tables, tbl.Name)
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

	// http://kubernetes.io/docs/api-reference/v1/definitions/#_v1_podstatus
	tbl.AddField(schema.NewFieldBase("phase", value.StringType, 256, "string"))
	tbl.AddField(schema.NewFieldBase("conditions", value.JsonType, 256, "json array"))
	tbl.AddField(schema.NewFieldBase("message", value.StringType, 256, "string"))
	tbl.AddField(schema.NewFieldBase("reason", value.StringType, 256, "string"))
	tbl.AddField(schema.NewFieldBase("hostip", value.StringType, 32, "string"))
	tbl.AddField(schema.NewFieldBase("podip", value.StringType, 32, "string"))
	tbl.AddField(schema.NewFieldBase("starttime", value.TimeType, 64, "datetime"))
	tbl.AddField(schema.NewFieldBase("containerstatuses", value.JsonType, 256, "json array"))
	colNames = append(colNames, []string{"phase", "conditions", "message", "reason",
		"hostip", "podip", "starttime", "containerstatuses"}...)

	// http://kubernetes.io/docs/api-reference/v1/definitions/#_v1_podspec
	tbl.AddField(schema.NewFieldBase("volumes", value.JsonType, 256, "json array"))
	tbl.AddField(schema.NewFieldBase("containers", value.JsonType, 256, "json array"))
	tbl.AddField(schema.NewFieldBase("restartpolicy", value.StringType, 256, "string"))
	tbl.AddField(schema.NewFieldBase("terminationgraceperiodseconds", value.IntType, 64, "long"))
	tbl.AddField(schema.NewFieldBase("activedeadlineseconds", value.IntType, 64, "long"))
	tbl.AddField(schema.NewFieldBase("dnspolicy", value.StringType, 256, "string"))
	tbl.AddField(schema.NewFieldBase("nodeselector", value.JsonType, 256, "json object"))
	tbl.AddField(schema.NewFieldBase("serviceaccountname", value.StringType, 256, "string"))
	// This appears deprecated
	//tbl.AddField(schema.NewFieldBase("serviceaccount", value.StringType, 256, "string"))
	tbl.AddField(schema.NewFieldBase("nodename", value.StringType, 32, "string"))
	tbl.AddField(schema.NewFieldBase("hostnetwork", value.BoolType, 1, "boolean"))
	tbl.AddField(schema.NewFieldBase("hostpid", value.BoolType, 1, "boolean"))
	tbl.AddField(schema.NewFieldBase("hostipc", value.BoolType, 1, "boolean"))
	tbl.AddField(schema.NewFieldBase("securitycontext", value.JsonType, 256, "json object"))
	tbl.AddField(schema.NewFieldBase("imagepullsecrets", value.JsonType, 256, "json array"))
	tbl.AddField(schema.NewFieldBase("hostname", value.StringType, 32, "string"))
	tbl.AddField(schema.NewFieldBase("subdomain", value.StringType, 32, "string"))
	colNames = append(colNames, []string{"volumes", "containers", "restartpolicy", "terminationgraceperiodseconds",
		"activedeadlineseconds", "dnspolicy", "nodeselector", "serviceaccountname",
		"nodename", "hostnetwork", "hostpid", "hostipc",
		"securitycontext", "imagepullsecrets", "hostname", "subdomain"}...)

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
	tbl.AddField(schema.NewFieldBase("labels", value.JsonType, 256, "object"))
	colNames = append(colNames, []string{"name", "generatename", "namespace", "selflink",
		"uid", "resourceversion", "generation", "creationtimestamp", "deletiontimestamp", "labels"}...)
	return colNames
}
