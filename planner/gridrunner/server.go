package gridrunner

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	u "github.com/araddon/gou"
	"github.com/sony/sonyflake"

	"github.com/lytics/grid/grid2"
	"github.com/lytics/grid/grid2/condition"
	"github.com/lytics/grid/grid2/ring"

	"github.com/araddon/qlbridge/plan"
)

var sf *sonyflake.Sonyflake

func init() {
	var st sonyflake.Settings
	st.StartTime = time.Now()
	sf = sonyflake.NewSonyflake(st)
}

func NextId() (uint64, error) {
	return sf.NextID()
}

type Server struct {
	Conf       *Conf
	g          grid2.Grid
	started    bool
	lastTaskId uint64
}

func (s *Server) SubmitTask(sp *plan.SourcePlan) interface{} {

	var err error
	taskId, err := sf.NextID()
	if err != nil {
		u.Errorf("Wat, no taskid? err=%v", err)
		return nil
	}

	flow := NewFlow(taskId)

	u.Debugf("%s starting job ", flow)

	// Attempting to find out when this task has finished
	// and want to go read Store() for getting state?
	f := condition.NewNameWatch(s.g.Etcd(), s.g.Name(), flow.Name(), "finished")
	finished := f.WatchUntil(flow.NewContextualName("leader"))

	rp := ring.New(flow.NewContextualName("producer"), s.Conf.NrProducers)
	for _, def := range rp.ActorDefs() {
		def.DefineType("producer")
		def.Define("flow", flow.Name())
		err := s.g.StartActor(def)
		if err != nil {
			u.Errorf("error: failed to start: %v, due to: %v", def, err)
			os.Exit(1)
		}
	}

	rc := ring.New(flow.NewContextualName("consumer"), s.Conf.NrConsumers)
	for _, def := range rc.ActorDefs() {
		def.DefineType("consumer")
		def.Define("flow", flow.Name())
		err := s.g.StartActor(def)
		if err != nil {
			u.Errorf("error: failed to start: %v, due to: %v", def, err)
			os.Exit(1)
		}
	}

	ldr := grid2.NewActorDef(flow.NewContextualName("leader"))
	ldr.DefineType("leader")
	ldr.Define("flow", flow.Name())
	err = s.g.StartActor(ldr)
	if err != nil {
		u.Errorf("error: failed to start: %v, due to: %v", "leader", err)
		os.Exit(1)
	}

	st := condition.NewState(s.g.Etcd(), 10*time.Minute, s.g.Name(), flow.Name(), "state", ldr.ID())
	defer st.Stop()

	// can we read the reader state?

	select {
	case <-finished:
		u.Warnf("%s YAAAAAY finished", flow.String())
		var state LeaderState
		if _, err := st.Fetch(&state); err != nil {
			u.Warnf("%s failed to read state: %v", flow, err)
		} else {
			u.Infof("%s got STATE: %#v", flow, state)
			return state
		}
	case <-time.After(30 * time.Second):
		u.Warnf("%s exiting bc timeout", flow)
	}
	return nil
}
func (s *Server) RunWorker() error {
	m, err := newActorMaker(s.Conf)
	if err != nil {
		u.Errorf("error: failed to make actor maker: %v", err)
		return err
	}
	return s.runMaker(m)
}
func (s *Server) RunMaster() error {
	u.Infof("start grid master")
	return s.runMaker(&nilMaker{})
}
func (s *Server) runMaker(actorMaker grid2.ActorMaker) error {

	// We are going to start a "Grid Master" which is only used for making requests
	// to etcd to start tasks
	s.g = grid2.NewGridDetails(s.Conf.GridName, s.Conf.Hostname, s.Conf.EtcdServers, s.Conf.NatsServers, actorMaker)

	exit, err := s.g.Start()
	if err != nil {
		u.Errorf("error: failed to start grid: %v", err)
		return fmt.Errorf("error starting grid master %v", err)
	}

	j := condition.NewJoin(s.g.Etcd(), 30*time.Second, s.g.Name(), "hosts", s.Conf.Hostname)
	err = j.Join()
	if err != nil {
		u.Errorf("error: failed to regester: %v", err)
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
					u.Errorf("error: failed to report liveness: %v", err)
					os.Exit(1)
				}
			}
		}
	}()

	w := condition.NewCountWatch(s.g.Etcd(), s.g.Name(), "hosts")
	defer w.Stop()

	u.Debugf("waiting for %d nodes to join", s.Conf.NodeCt)
	started := w.WatchUntil(s.Conf.NodeCt)
	select {
	case <-exit:
		u.Debug("Shutting down, grid exited")
		return nil
	case <-w.WatchError():
		u.Errorf("error: failed to watch other hosts join: %v", err)
		os.Exit(1)
	case <-started:
		s.started = true
		u.Infof("now started")
	}

	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
		select {
		case <-sig:
			u.Debug("shutting down")
			s.g.Stop()
		case <-exit:
		}
	}()

	<-exit
	u.Info("shutdown complete")
	return nil
}

type Flow string

func NewFlow(nr uint64) Flow {
	return Flow(fmt.Sprintf("flow-%v", nr))
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
