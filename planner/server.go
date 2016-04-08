package planner

import (
	"encoding/base64"
	"fmt"
	"os"
	"strconv"
	"time"

	u "github.com/araddon/gou"
	"github.com/lytics/grid"
	"github.com/lytics/grid/condition"
	"github.com/lytics/grid/ring"
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

// Distributed DataUx worker node
type Server struct {
	Conf       *Conf
	reg        *datasource.Registry
	Grid       grid.Grid
	started    bool
	lastTaskId uint64
}

func (m *Server) startSqlActor(actorCt, actorId int, partition string, pb string,
	flow Flow, def *grid.ActorDef, p *plan.Select) error {

	def.DefineType("sqlactor")
	def.Define("flow", flow.Name())
	def.Settings["pb64"] = pb
	def.Settings["partition"] = partition
	def.Settings["actor_ct"] = strconv.Itoa(actorCt)
	//u.Debugf("%p submitting start actor %s  nodeI=%d", m, def.ID(), actorId)
	err := m.Grid.StartActor(def)
	//u.Debugf("%p after submit start actor", m)
	if err != nil {
		u.Errorf("error: failed to start: %v, due to: %v", "sqlactor", err)
	}
	return err
}

func (m *Server) SubmitTask(localTask exec.TaskRunner, flow Flow, p *plan.Select) error {

	//u.Debugf("%p master submitting job childdag?%v  %s", m, p.ChildDag, p.Stmt.String())
	//u.LogTracef(u.WARN, "hello")
	// marshal plan to Protobuf for transport
	pb, err := p.Marshal()
	if err != nil {
		u.Errorf("Could not protbuf marshal %v for %s", err, p.Stmt)
		return err
	}
	// TODO:  send the instructions as a grid message NOT part of actor-def
	pb64 := base64.URLEncoding.EncodeToString(pb)
	//u.Infof("pb64:  %s", pb64)

	actorCt := 1
	partitions := []string{""}
	if len(p.Stmt.With) > 0 && p.Stmt.With.Bool("distributed") {
		//u.Warnf("distribution instructions node_ct:%v", p.Stmt.With.Int("node_ct"))
		for _, f := range p.From {
			if f.Tbl != nil {
				if f.Tbl.Partition != nil {
					partitions = make([]string, len(f.Tbl.Partition.Partitions))
					actorCt = len(f.Tbl.Partition.Partitions)
					for i, part := range f.Tbl.Partition.Partitions {
						//u.Warnf("Found Partitions for %q = %#v", f.Tbl.Name, part)
						partitions[i] = part.Id
					}
				} else if f.Tbl.PartitionCt > 0 {
					partitions = make([]string, f.Tbl.PartitionCt)
					actorCt = f.Tbl.PartitionCt
					for i := 0; i < actorCt; i++ {
						//u.Warnf("Found Partitions for %q = %#v", f.Tbl.Name, i)
						partitions[i] = fmt.Sprintf("%d", i)
					}
				} else {
					u.Warnf("partition? %#v", f.Tbl.Partition)
				}
			}
		}
	} else {
		u.Warnf("TODO:  NOT Distributed, don't start tasks!")
	}

	_, err = m.Grid.Etcd().CreateDir(fmt.Sprintf("/%v/%v/%v", m.Grid.Name(), flow.Name(), "sqlcomplete"), 100000)
	if err != nil {
		u.Errorf("Could not initilize dir %v", err)
	}
	_, err = m.Grid.Etcd().CreateDir(fmt.Sprintf("/%v/%v/%v", m.Grid.Name(), flow.Name(), "finished"), 100000)
	if err != nil {
		u.Errorf("Could not initilize dir %v", err)
	}

	w := condition.NewCountWatch(m.Grid.Etcd(), m.Grid.Name(), flow.Name(), "finished")
	defer w.Stop()

	finished := w.WatchUntil(actorCt)

	rp := ring.New(flow.NewContextualName("sqlactor"), actorCt)
	u.Debugf("%p master?? submitting distributed sql query with %d actors", m, actorCt)
	//u.WarnT(18)
	for i, def := range rp.ActorDefs() {
		go func(ad *grid.ActorDef, actorId int) {
			if err = m.startSqlActor(actorCt, actorId, partitions[actorId], pb64, flow, ad, p); err != nil {
				u.Errorf("Could not create sql actor %v", err)
			}
		}(def, i)
	}

	//u.Debugf("submitted actors, waiting for completion signal")
	select {
	case <-finished:
		//u.Debugf("%s got all! finished signal?", flow.Name())
		return nil
	case <-localTask.SigChan():
		u.Warnf("%s YAAAAAY finished", flow.String())

		//case <-time.After(30 * time.Second):
		//	u.Warnf("%s exiting bc timeout", flow)
	}
	u.Warnf("what is going on?")
	return nil
}

func (m *Server) RunWorker(quit chan bool) error {
	//u.Debugf("%p starting grid worker", m)
	actor, err := newActorMaker(m.Conf)
	if err != nil {
		u.Errorf("failed to make actor maker: %v", err)
		return err
	}
	return m.runMaker(quit, actor)
}

func (m *Server) RunMaster(quit chan bool) error {
	//u.Debugf("%p start grid master", m)
	return m.runMaker(quit, &nilMaker{})
}

func (s *Server) runMaker(quit chan bool, actorMaker grid.ActorMaker) error {

	// We are going to start a "Grid" with specified maker
	//   - nilMaker = "master" only used for submitting tasks, not performing them
	//   - normal maker;  performs specified work units
	s.Grid = grid.New(s.Conf.GridName, s.Conf.Hostname, s.Conf.EtcdServers, s.Conf.NatsServers, actorMaker)

	u.Debugf("created new distributed grid sql job maker: %#v", s.Grid)
	exit, err := s.Grid.Start()
	if err != nil {
		u.Errorf("failed to start grid: %v", err)
		return fmt.Errorf("error starting grid %v", err)
	}

	defer func() {
		u.Debugf("defer grid worker complete: %s", s.Conf.Hostname)
		s.Grid.Stop()
	}()

	complete := make(chan bool)

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
			case <-quit:
				u.Debugf("quit signal")
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

	w := condition.NewCountWatch(s.Grid.Etcd(), s.Grid.Name(), "hosts")
	defer w.Stop()

	waitForCt := s.Conf.NodeCt + 1 // worker nodes + master
	//u.Debugf("%p waiting for %d nodes to join", s, waitForCt)
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
		s.started = true
		//u.Debugf("%p now started", s)
	}
	//u.Debug("waiting for exit")
	<-exit
	//u.Debug("shutdown complete")
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
