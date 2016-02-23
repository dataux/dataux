package planner

import (
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	u "github.com/araddon/gou"
	"github.com/lytics/grid"
	"github.com/lytics/grid/condition"
	"github.com/sony/sonyflake"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/plan"
)

var sf *sonyflake.Sonyflake

func init() {
	var st sonyflake.Settings
	// TODO, ensure we get a unique etcdid for machineid
	st.StartTime = time.Now()
	sf = sonyflake.NewSonyflake(st)
	// Lets use our distributed generator
	plan.NextId = NextIdUnsafe
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

type Server struct {
	Conf       *Conf
	schemaconf *datasource.RuntimeSchema
	Grid       grid.Grid
	started    bool
	lastTaskId uint64
}

func (m *Server) startSqlActor(nodeCt int, partition string, pb []byte, flow Flow, p *plan.Select) error {
	sqlNode := grid.NewActorDef(flow.NewContextualName("sqlactor"))
	sqlNode.DefineType("sqlactor")
	sqlNode.Define("flow", flow.Name())
	sqlNode.RawData["pb"] = pb
	sqlNode.Settings["partition"] = partition
	sqlNode.Settings["node_ct"] = strconv.Itoa(nodeCt)
	u.Debugf("%p submitting start actor", m)
	err := m.Grid.StartActor(sqlNode)
	u.Debugf("%p after submit start actor", m)
	if err != nil {
		u.Errorf("error: failed to start: %v, due to: %v", "sqlactor", err)
		//os.Exit(1)
	}
	return err
}

func (m *Server) SubmitTask(localTask exec.TaskRunner, flow Flow, p *plan.Select) error {

	u.Debugf("%p master starting job %s", m, flow)

	// marshal plan to Protobuf for transport
	pb, err := p.Marshal()
	if err != nil {
		u.Errorf("Could not protbuf marshal %v for %s", err, p.Stmt)
		return err
	}
	//pbsBase := base64.URLEncoding.EncodeToString(pb)
	//sqlNode.Settings["pb"] = pbsBase

	nodeCt := 1
	partitions := []string{""}
	if len(p.Stmt.With) > 0 && p.Stmt.With.Bool("distributed") {
		//u.Warnf("distribution instructions node_ct:%v", p.Stmt.With.Int("node_ct"))
		for _, f := range p.From {
			if f.Tbl != nil {
				if len(f.Tbl.Partitions) > 1 {
					partitions = make([]string, len(f.Tbl.Partitions))
					nodeCt = len(f.Tbl.Partitions)
					for i, part := range f.Tbl.Partitions {
						//u.Warnf("Found Partitions for %q = %#v", f.Tbl.Name, part)
						partitions[i] = part.Id
					}
				}
			}
		}
	}

	for i := 0; i < nodeCt; i++ {
		go func(nodeId int) {
			if err = m.startSqlActor(nodeCt, partitions[nodeId], pb, flow, p); err != nil {
				u.Errorf("Could not create sql actor %v", err)
			}
		}(i)

	}
	select {
	case <-localTask.SigChan():
		u.Warnf("%s YAAAAAY finished", flow.String())

	case <-time.After(30 * time.Second):
		u.Warnf("%s exiting bc timeout", flow)
	}
	return nil
}
func (m *Server) RunWorker() error {
	u.Infof("%p starting grid worker", m)
	actor, err := newActorMaker(m.Conf)
	if err != nil {
		u.Errorf("failed to make actor maker: %v", err)
		return err
	}
	return m.runMaker(actor)
}
func (m *Server) RunMaster() error {
	u.Infof("%p start grid master", m)
	return m.runMaker(&nilMaker{})
}
func (s *Server) runMaker(actorMaker grid.ActorMaker) error {

	// We are going to start a "Grid" with specified maker
	//   - nilMaker = "master" only used for submitting tasks, not performing them
	//   - normal maker;  performs specified work units
	s.Grid = grid.New(s.Conf.GridName, s.Conf.Hostname, s.Conf.EtcdServers, s.Conf.NatsServers, actorMaker)

	exit, err := s.Grid.Start()
	if err != nil {
		u.Errorf("failed to start grid: %v", err)
		return fmt.Errorf("error starting grid %v", err)
	}

	j := condition.NewJoin(s.Grid.Etcd(), 30*time.Second, s.Grid.Name(), "hosts", s.Conf.Hostname)
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
			case <-exit:
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

	w := condition.NewCountWatch(s.Grid.Etcd(), s.Grid.Name(), "hosts")
	defer w.Stop()

	waitForCt := s.Conf.NodeCt + 1 // worker nodes + master
	u.Debugf("%p waiting for %d nodes to join", s, waitForCt)
	//u.LogTraceDf(u.WARN, 16, "")
	started := w.WatchUntil(waitForCt)
	select {
	case <-exit:
		//u.Debug("Shutting down, grid exited")
		return nil
	case <-w.WatchError():
		u.Errorf("failed to watch other hosts join: %v", err)
		os.Exit(1)
	case <-started:
		s.started = true
		u.Infof("%p now started", s)
	}

	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
		select {
		case <-sig:
			//u.Debug("shutting down")
			s.Grid.Stop()
		case <-exit:
		}
	}()

	<-exit
	//u.Info("shutdown complete")
	return nil
}

type Flow string

func NewFlow(nr uint64) Flow {
	return Flow(fmt.Sprintf("sql-%v", nr))
}

func (f Flow) NewContextualName(name string) string {
	return fmt.Sprintf("%v-%v", f, name)
}

func (f Flow) Name() string {
	return string(f)
}

func (f Flow) String() string {
	return string(f)
}
