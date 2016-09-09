package planner

import (
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	u "github.com/araddon/gou"
	"github.com/lytics/grid"
	"github.com/lytics/grid/condition"
	"github.com/lytics/metafora"
	"github.com/sony/sonyflake"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/plan"
)

var (
	loggingOnce sync.Once

	// BuiltIn Default Conf, used for testing but real runtime swaps this out
	//  for a real config
	GridConf = &Conf{
		GridName:    "dataux",
		EtcdServers: strings.Split("http://127.0.0.1:2379", ","),
		NatsServers: strings.Split("nats://127.0.0.1:4222", ","),
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
	metafora.SetLogger(u.GetLogger()) // Configure metafora's logger
	metafora.SetLogLevel(metafora.LogLevelWarn)
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
	Grid       grid.Grid
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
func (m *PlannerGrid) RunSqlMaster(completionTask exec.TaskRunner, ns *SourceNats, flow Flow, p *plan.Select) error {

	t := newSqlMasterTask(m, completionTask, ns, flow, p)
	return t.Run()
}

func (m *PlannerGrid) Run(quit chan bool) error {

	// We are going to start a Grid Client for managing our remote tasks
	m.Grid = grid.NewClient(m.Conf.GridName, m.Conf.Hostname, m.Conf.EtcdServers, m.Conf.NatsServers)

	//u.Debugf("%p created new distributed grid sql job maker: %#v", m, m.Grid)
	exit, err := m.Grid.Start()
	if err != nil {
		u.Errorf("failed to start grid: %v", err)
		return fmt.Errorf("error starting grid %v", err)
	}

	defer func() {
		u.Debugf("defer grid server complete: %s", m.Conf.Hostname)
		m.Grid.Stop()
	}()

	complete := make(chan bool)

	j := condition.NewJoin(m.Grid.Etcd(), 30*time.Second, m.Grid.Name(), "hosts", m.Conf.Hostname)
	err = j.Join()
	if err != nil {
		u.Errorf("failed to register grid node: %v", err)
		os.Exit(1)
	}
	defer j.Exit()
	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-quit:
				//u.Debugf("quit signal")
				close(complete)
				return
			case <-exit:
				//u.Debugf("worker grid exit??")
				return
			case <-ticker.C:
				err := j.Alive()
				if err != nil {
					u.Errorf("failed to report liveness: %v", err)
					os.Exit(1)
				}
			}
		}
	}()

	w := condition.NewCountWatch(m.Grid.Etcd(), m.Grid.Name(), "hosts")
	defer w.Stop()

	waitForCt := m.Conf.NodeCt + 1 // worker nodes + master
	//u.Debugf("%p waiting for %d nodes to join", m, waitForCt)
	//u.LogTraceDf(u.WARN, 16, "")
	started := w.WatchUntil(waitForCt)
	select {
	case <-complete:
		u.Debugf("got complete signal")
		return nil
	case <-exit:
		//u.Debug("Shutting down, grid exited")
		return nil
	case <-w.WatchError():
		u.Errorf("failed to watch other hosts join: %v", err)
		os.Exit(1)
	case <-started:
		m.started = true
		//u.Debugf("%p now started", m)
	}
	<-exit
	//u.Debug("shutdown complete")
	return nil
}
