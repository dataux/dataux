package planner

import (
	"fmt"
	"net"
	"os"
	"runtime"
	"strings"
	"time"

	u "github.com/araddon/gou"
	etcdv3 "github.com/coreos/etcd/clientv3"
	"github.com/lytics/grid/grid.v3"
	"github.com/sony/sonyflake"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/plan"
)

var (
	// BuiltIn Default Conf, used for testing but real runtime swaps this out
	// for a real config
	GridConf = &Conf{
		GridName:    "dataux",
		Address:     "localhost:0",
		EtcdServers: strings.Split("http://127.0.0.1:2379", ","),
	}

	// Unique id service
	sf *sonyflake.Sonyflake
)

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	var st sonyflake.Settings
	// TODO, ensure we get a unique etcdid for machineid
	st.StartTime = time.Now()
	sf = sonyflake.NewSonyflake(st)
	// Lets use our distributed generator
	plan.NextId = NextIdUnsafe
}

func setupLogging() {
	u.DiscardStandardLogger() // Discard non-sanctioned spammers
}

func NextIdUnsafe() uint64 {
	uv, err := NextId()
	if err != nil {
		u.Errorf("error generating nextId %v", err)
	}
	return uv
}

func NextId() (uint64, error) {
	return sf.NextID()
}

func NodeName(id uint64) string {
	hostname, err := os.Hostname()
	if err != nil {
		u.Errorf("error: failed to discover hostname: %v", err)
	}
	return fmt.Sprintf("%s-%d", hostname, id)
}

func NodeName2(id1, id2 uint64) string {
	hostname, err := os.Hostname()
	if err != nil {
		u.Errorf("error: failed to discover hostname: %v", err)
	}
	return fmt.Sprintf("%s-%d-%d", hostname, id1, id2)
}

// PlannerGrid that manages the sql tasks, workers
type PlannerGrid struct {
	Conf       *Conf
	reg        *datasource.Registry
	GridServer *grid.Server
	gridClient *grid.Client
	started    bool
	lastTaskId uint64
}

func NewServerPlanner(nodeCt int, r *datasource.Registry) *PlannerGrid {
	nextId, _ := NextId()

	conf := GridConf.Clone()
	conf.NodeCt = nodeCt
	conf.Hostname = NodeName(nextId)
	return &PlannerGrid{Conf: conf, reg: r}
}

// Submits a Sql Select statement task for planning across multiple nodes
func (m *PlannerGrid) RunSqlTask(completionTask exec.TaskRunner, ns *Source, flow Flow, p *plan.Select) error {

	t := newSqlMasterTask(m, completionTask, ns, flow, p)
	return t.Run()
}

func (m *PlannerGrid) Run(quit chan bool) error {

	logger := u.GetLogger()

	// Connect to etcd.
	etcd, err := etcdv3.New(etcdv3.Config{Endpoints: m.Conf.EtcdServers})
	if err != nil {
		u.Errorf("failed to start etcd client: %v", err)
		return err
	}

	// Create a grid client.
	m.gridClient, err = grid.NewClient(etcd, grid.ClientCfg{Namespace: "dataux", Logger: logger})
	if err != nil {
		u.Errorf("failed to start grid client: %v", err)
		return err
	}

	// Create a grid server.
	m.GridServer, err = grid.NewServer(etcd, grid.ServerCfg{Namespace: "dataux", Logger: logger})
	if err != nil {
		u.Errorf("failed to start grid server: %v", err)
		return err
	}

	// Define how actors are created.
	m.GridServer.RegisterDef("leader", LeaderCreate(m.gridClient))
	m.GridServer.RegisterDef("sqlworker", WorkerFactory(m.gridClient, m.GridServer))

	lis, err := net.Listen("tcp", m.Conf.Address)
	if err != nil {
		u.Errorf("failed to start tcp listener server: %v", err)
		return err
	}

	// Blocking call to serve
	err = m.GridServer.Serve(lis)
	if err != nil {
		u.Errorf("grid serve failed with error=%v", err)
		return err
	}
	return nil
}
