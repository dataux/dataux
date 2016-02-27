package planner

import (
	"encoding/gob"
	"os"
	"strconv"
	"time"

	u "github.com/araddon/gou"

	"github.com/lytics/dfa"
	"github.com/lytics/grid"
	"github.com/lytics/grid/condition"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/plan"
)

func init() {
	gob.Register(datasource.SqlDriverMessageMap{})
	//gob.Register(DataMsg{})
}

type (
	// State of a single actor, persisted upon stop, transfer nodes
	SqlState struct {
		Start          time.Time
		ConsumerCounts map[string]int
		ProducerCounts map[string]int
	}
	// Our actor for running SQL tasks in distributed grid nodes
	SqlActor struct {
		def      *grid.ActorDef
		conf     *Conf
		flow     Flow
		grid     grid.Grid
		rx       grid.Receiver
		tx       grid.Sender
		exit     <-chan bool
		started  condition.Join
		finished condition.Join
		state    *SqlState

		// Non Grid Sql state
		p           *plan.Select
		et          exec.TaskRunner
		sqlJobMaker interface{}
	}
)

func NewSqlState() *SqlState {
	return &SqlState{ConsumerCounts: make(map[string]int), ProducerCounts: make(map[string]int)}
}

func NewSqlActor(def *grid.ActorDef, conf *Conf) grid.Actor {
	sa := &SqlActor{
		def:  def,
		conf: conf,
		flow: Flow(def.Settings["flow"]),
	}
	//u.Debugf("%p new sqlactor", sa)
	return sa
}

func (a *SqlActor) ID() string {
	return a.def.ID()
}

func (a *SqlActor) String() string {
	return a.ID()
}

func (a *SqlActor) Act(g grid.Grid, exit <-chan bool) bool {
	rx, err := grid.NewReceiver(g.Nats(), a.ID(), 4, 0)
	if err != nil {
		u.Errorf("%v: error: %v", a.ID(), err)
	}
	defer rx.Close()

	a.rx = rx
	a.grid = g
	a.exit = exit

	d := dfa.New()
	d.SetStartState(Starting)
	d.SetTerminalStates(Exiting, Terminating)
	d.SetTransitionLogger(func(state dfa.State) {
		//u.Infof("%v: switched to state: %v", a, state)
	})

	d.SetTransition(Starting, EverybodyStarted, Running, a.Running)
	d.SetTransition(Starting, EverybodyFinished, Terminating, a.Terminating)
	d.SetTransition(Starting, Failure, Exiting, a.Exiting)
	d.SetTransition(Starting, Exit, Exiting, a.Exiting)

	d.SetTransition(Running, EverybodyFinished, Finishing, a.Finishing)
	d.SetTransition(Running, Failure, Exiting, a.Exiting)
	d.SetTransition(Running, Exit, Exiting, a.Exiting)

	d.SetTransition(Finishing, EverybodyFinished, Terminating, a.Terminating)
	d.SetTransition(Finishing, Failure, Exiting, a.Exiting)
	d.SetTransition(Finishing, Exit, Exiting, a.Exiting)

	final, _ := d.Run(a.Starting)
	u.Warnf("%s sqlactor final: %v", a, final.String())
	if final == Terminating {
		return true
	}
	return false
}

func (m *SqlActor) Starting() dfa.Letter {

	//u.Debugf("%p settings: %v", m, m.def.Settings)
	//u.LogTracef(u.WARN, "wat")
	nodeCt := 1
	nodeCt64, err := strconv.ParseInt(m.def.Settings["node_ct"], 10, 64)
	if err == nil && nodeCt64 > 0 {
		nodeCt = int(nodeCt64)
	}
	pb := m.def.RawData["pb"]

	p, err := plan.SelectPlanFromPbBytes(pb, m.conf.SchemaLoader)
	if err != nil {
		u.Errorf("error %v", err)
		return Failure
	}
	m.p = p
	p.Ctx.DisableRecover = m.conf.SupressRecover

	if m.conf == nil {
		u.Warnf("no conf?")
		return Failure
	}
	if m.conf.JobMaker == nil {
		u.Warnf("no JobMaker?")
		return Failure
	}
	executor, err := m.conf.JobMaker(p.Ctx)
	if err != nil {
		u.Errorf("error %v", err)
		return Failure
	}

	//u.Debugf("nodeCt:%v  run executor walk select %#v from ct? %v", nodeCt, p.Stmt.With, len(p.From))
	for _, f := range p.From {
		f.Custom["partition"] = m.def.Settings["partition"]
		//u.Infof("from: %#v", f.Custom)
	}
	//u.Infof("%p starting executor %#v", m, executor)
	sqlTask, err := executor.WalkSelectPartition(p, nil)
	//sqlTask, err := executor.WalkPlan(p)
	if err != nil {
		u.Errorf("Could not create select task %v", err)
		return Failure
	}
	taskRunner, ok := sqlTask.(exec.TaskRunner)
	if !ok {
		u.Errorf("Expected exec.TaskRunner but got %T", sqlTask)
		return Failure
	}
	m.et = taskRunner

	tx, err := grid.NewSender(m.grid.Nats(), 1)
	if err != nil {
		u.Errorf("error: %v", err)
		return Failure
	}
	m.tx = tx

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	//time.Sleep(3 * time.Second)

	//u.Debugf("%p starting actor %#v  settings:%v", m, m.flow.Name(), m.def.Settings)

	// Our Join Barrier that is going to allow us to wait for all
	//  sql actors to have started
	j := condition.NewJoin(m.grid.Etcd(), 2*time.Minute, m.grid.Name(), m.flow.Name(), "started", m.ID())
	if err := j.Rejoin(); err != nil {
		u.Errorf("wat?  %v", err)
		return Failure
	}
	m.started = j

	w := condition.NewCountWatch(m.grid.Etcd(), m.grid.Name(), m.flow.Name(), "started")
	defer w.Stop()

	f := condition.NewNameWatch(m.grid.Etcd(), m.grid.Name(), m.flow.Name(), "finished")
	defer f.Stop()

	//started := w.WatchUntil(m.conf.NrConsumers + m.conf.NrProducers + 1)
	//u.Infof("%p waiting for %v", m, nodeCt)
	// if 0 == 0 {
	// 	return EverybodyStarted
	// }
	started := w.WatchUntil(nodeCt)
	finished := f.WatchUntil(m.flow.NewContextualName("sqlactor"))
	for {
		select {
		case <-m.exit:
			return Exit
		case <-ticker.C:
			if err := m.started.Alive(); err != nil {
				return Failure
			}
		case <-started:
			//u.Debugf("%p everybody started", m)
			return EverybodyStarted
		case <-finished:
			u.Warnf("everybody finished?")
			return EverybodyFinished
		}
	}
}

func (m *SqlActor) RunSqlDag() {

	natsSink := NewSinkNats(m.p.Ctx, m.flow.Name(), m.tx)
	m.et.Add(natsSink)
	m.et.Setup(0)

	err := m.et.Run()
	if err != nil {
		u.Errorf("error on Query.Run(): %v", err)
	}

	j := condition.NewJoin(m.grid.Etcd(), 10*time.Second, m.grid.Name(), m.flow.Name(), "sqlcomplete", m.ID())
	u.Infof("sending sqlcomplete join message for %#v", m.et)
	if err = j.Rejoin(); err != nil {
		u.Errorf("could not join?? %v", err)
	}
	u.Warnf("sqlcomplete sql dag")
	if 22 == 55 {
		os.Exit(0)
	}
}

func (m *SqlActor) Finishing() dfa.Letter {

	u.Warnf("%s sqlactor finishing", m.String())
	return EverybodyFinished

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	j := condition.NewJoin(m.grid.Etcd(), 10*time.Minute, m.grid.Name(), m.flow.Name(), "finished")
	if err := j.Rejoin(); err != nil {
		return Failure
	}
	m.finished = j

	w := condition.NewCountWatch(m.grid.Etcd(), m.grid.Name(), m.flow.Name(), "finished")
	defer w.Stop()

	// finished := w.WatchUntil(m.conf.NrConsumers + m.conf.NrProducers + 1)
	finished := w.WatchUntil(1)
	for {
		select {
		case <-m.exit:
			return Exit
		case <-ticker.C:
			if err := m.started.Alive(); err != nil {
				return Failure
			}
			if err := m.finished.Alive(); err != nil {
				return Failure
			}
		case <-finished:
			m.started.Exit()
			m.finished.Alive()
			return EverybodyFinished
		case err := <-w.WatchError():
			u.Errorf("%v: error: %v", m, err)
			return Failure
		}
	}
}

func (m *SqlActor) Running() dfa.Letter {

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	m.state = NewSqlState()
	s := condition.NewState(m.grid.Etcd(), 10*time.Minute, m.grid.Name(), m.flow.Name(), "state", m.ID())
	defer s.Stop()

	if err := s.Init(m.state); err != nil {
		if _, err := s.Fetch(m.state); err != nil {
			return FetchStateFailure
		}
	}

	//u.Debugf("%v: running ", m.ID())

	w := condition.NewCountWatch(m.grid.Etcd(), m.grid.Name(), m.flow.Name(), "sqlcomplete")
	defer w.Stop()

	// Now run the Actual worker
	go m.RunSqlDag()

	finished := w.WatchUntil(1)
	for {
		select {
		case <-m.exit:
			u.Warnf("%s finished store", m)
			return Exit
		case <-ticker.C:
			u.Infof("alive")
			if err := m.started.Alive(); err != nil {
				u.Warnf("sqlactor not alive?: %v", err)
				return Failure
			}
			u.Warnf("%s about to do ticker store", m)
		case <-finished:
			u.Warnf("%s sqlactor about to send finished signal?", m)
			return EverybodyFinished
		case err := <-w.WatchError():
			u.Errorf("%v: error: %v", m, err)
			return Failure
		case m := <-m.rx.Msgs():

			// Like any actor we can recieve normal messages
			// in this situation we have actors report back metrics to us
			// for this sample, we have some count of messages in state, but normally
			// you would have progress (kafkaId's, offsetids, status) in state
			// and metrics here
			u.Debugf("%s rx Msg:  %#v", m, m)
			// switch m := m.(type) {
			// case ResultMsg:
			// }
		}
	}
}

func (m *SqlActor) Exiting() {
	if m.started != nil {
		m.started.Stop()
	}
	if m.finished != nil {
		m.finished.Stop()
	}
	if m.et != nil {
		m.et.Close()
	}
}

func (m *SqlActor) Terminating() {
	u.Warnf("%s SqlActor terminating", m)
	m.Exiting()

	if m.state == nil {
		u.Warnf("%s nil state?", m)
		return
	}

	for p, n := range m.state.ProducerCounts {
		rx := m.state.ConsumerCounts[p]
		delta := m.state.ConsumerCounts[p] - n
		u.Infof("%v: producer: %v, sent: %v, consumers received: %v, delta: %v", m.ID(), p, n, rx, delta)
	}
}
