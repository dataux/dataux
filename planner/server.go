package planner

import (
	"encoding/base64"
	"fmt"
	"os"
	"os/signal"
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

func (s *Server) SubmitTask(localTask exec.TaskRunner, flow Flow, p *plan.Select) error {

	u.Debugf("%s starting job server.Conf? %p", flow, s.Conf)

	// Going to marshal to Protobuf
	pb, err := p.Marshal()
	if err != nil {
		u.Errorf("Could not protbuf marshall %v for %s", err, p.Stmt)
		return err
	}
	//u.Infof("pb?  %s", pb)
	pbs := string(pb)
	p2, err := plan.SelectPlanFromPbBytes([]byte(pbs))
	if err != nil {
		u.Warnf("%v", []byte(pb))
		os.Exit(1)
		u.Errorf("error %v", err)
	}
	if !p.Equal(p2) {
		u.Warnf("wtf")
		os.Exit(1)
	} else {
		//u.Errorf("WTF?  WTF?, %v", pb)
		//u.Infof("p2? %#v", p2.Ctx)
	}

	//p2, err := plan.SelectPlanFromPbBytes(pb)

	/*
		sqlTask, err := m.JobExecutor.WalkSelect(p)
		if err != nil {
			u.Errorf("Could not create select task %v", err)
			return nil, err
		}

		tx, err := grid.NewSender(m.GridServer.Grid.Nats(), 1)
		if err != nil {
			u.Errorf("error: %v", err)
		}
		natsSink := NewSinkNats(m.Ctx, flow.Name(), tx)
		sqlTask.Add(natsSink)
	*/

	ldr := grid.NewActorDef(flow.NewContextualName("leader"))
	ldr.DefineType("leader")
	ldr.Define("flow", flow.Name())
	pbsBase := base64.URLEncoding.EncodeToString(pb)
	ldr.Settings["pb"] = pbsBase
	//u.Debugf("pbval: %v", ldr.Settings["pb"])
	err = s.Grid.StartActor(ldr)
	if err != nil {
		u.Errorf("error: failed to start: %v, due to: %v", "leader", err)
		os.Exit(1)
	}

	select {
	case <-localTask.SigChan():
		u.Warnf("%s YAAAAAY finished", flow.String())

	case <-time.After(30 * time.Second):
		u.Warnf("%s exiting bc timeout", flow)
	}
	return nil
}
func (s *Server) RunWorker() error {
	//u.Infof("starting grid worker nats: %v", s.Conf.NatsServers)
	m, err := newActorMaker(s.Conf)
	if err != nil {
		u.Errorf("failed to make actor maker: %v", err)
		return err
	}
	return s.runMaker(m)
}
func (s *Server) RunMaster() error {
	//u.Infof("start grid master")
	return s.runMaker(&nilMaker{})
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

	//u.Debugf("waiting for %d nodes to join", s.Conf.NodeCt)
	//u.LogTraceDf(u.WARN, 16, "")
	started := w.WatchUntil(s.Conf.NodeCt)
	select {
	case <-exit:
		//u.Debug("Shutting down, grid exited")
		return nil
	case <-w.WatchError():
		u.Errorf("failed to watch other hosts join: %v", err)
		os.Exit(1)
	case <-started:
		s.started = true
		//u.Infof("now started")
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
