package planner

import (
	"encoding/base64"
	"encoding/gob"
	"os"
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
	SqlState struct {
		Start             time.Time
		ConsumerCounts    map[string]int
		ProducerCounts    map[string]int
		ProducerDurations map[string]float64
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
	return &SqlState{ConsumerCounts: make(map[string]int), ProducerCounts: make(map[string]int), ProducerDurations: make(map[string]float64)}
}

func NewSqlActor(def *grid.ActorDef, conf *Conf) grid.Actor {
	//u.Debugf("%p conf", conf)
	return &SqlActor{
		def:  def,
		conf: conf,
		flow: Flow(def.Settings["flow"]),
	}
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
	u.Warnf("%s leader final: %v", a, final.String())
	if final == Terminating {
		return true
	}
	return false
}

func (m *SqlActor) Starting() dfa.Letter {

	//u.Debugf("pbval: %v", m.def.Settings["pb"])
	pb, err := base64.URLEncoding.DecodeString(m.def.Settings["pb"])
	if err != nil {
		u.Errorf("What, encoding error? %v", err)
		return Failure
	}

	//u.Infof("got pb? %T  \n%s", pb, pb)
	p, err := plan.SelectPlanFromPbBytes(pb, m.conf.SchemaLoader)
	if err != nil {
		u.Warnf("%v", pb)
		os.Exit(1)
		u.Errorf("error %v", err)
		return Failure
	}
	m.p = p
	//u.Infof("ctx: %+v", p.Ctx)
	//os.Exit(1)

	if m.conf == nil {
		u.Warnf("no conf?")
		os.Exit(1)
	}
	if m.conf.JobMaker == nil {
		u.Warnf("no JobMaker?")
		os.Exit(1)
	}
	executor, err := m.conf.JobMaker(p.Ctx)
	if err != nil {
		u.Errorf("error %v", err)
		return Failure
	}

	sqlTask, err := executor.WalkSelect(p)
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

	u.Infof("starting actor %#v", m.flow.Name())

	j := condition.NewJoin(m.grid.Etcd(), 2*time.Minute, m.grid.Name(), m.flow.Name(), "started", m.ID())
	if err := j.Rejoin(); err != nil {
		return Failure
	}
	m.started = j

	w := condition.NewCountWatch(m.grid.Etcd(), m.grid.Name(), m.flow.Name(), "started")
	defer w.Stop()

	f := condition.NewNameWatch(m.grid.Etcd(), m.grid.Name(), m.flow.Name(), "finished")
	defer f.Stop()

	//started := w.WatchUntil(m.conf.NrConsumers + m.conf.NrProducers + 1)
	started := w.WatchUntil(2)
	finished := f.WatchUntil(m.flow.NewContextualName("leader"))
	for {
		select {
		case <-m.exit:
			return Exit
		case <-ticker.C:
			if err := m.started.Alive(); err != nil {
				return Failure
			}
		case <-started:
			return EverybodyStarted
		case <-finished:
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
}

func (m *SqlActor) Finishing() dfa.Letter {

	u.Warnf("%s leader finishing", m.String())
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
	u.Infof("%v: running with state: %v, index: %v", m.ID(), m.state, s.Index())

	w := condition.NewCountWatch(m.grid.Etcd(), m.grid.Name(), m.flow.Name(), "sqlcomplete")
	defer w.Stop()

	// Now run the Actual worker
	go m.RunSqlDag()

	finished := w.WatchUntil(1)
	for {
		select {
		case <-m.exit:
			if _, err := s.Store(m.state); err != nil {
				u.Warnf("%v: failed to save state: %v", m, err)
			}
			u.Warnf("%s finished store", m)
			return Exit
		case <-ticker.C:
			u.Infof("alive")
			if err := m.started.Alive(); err != nil {
				u.Warnf("leader not alive?: %v", err)
				return Failure
			}
			u.Warnf("%s about to do ticker store", m)
			if _, err := s.Store(m.state); err != nil {
				u.Warnf("could not store: %v", err)
				return Failure
			}
		case <-finished:
			u.Warnf("%s leader about to send finished signal?", m)
			if _, err := s.Store(m.state); err != nil {
				u.Warnf("%v: failed to save state: %v", m, err)
			}
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
		rate := float64(m.state.ProducerCounts[p]) / m.state.ProducerDurations[p]
		u.Infof("%v: producer: %v, sent: %v, consumers received: %v, delta: %v, rate: %2.f/s", m.ID(), p, n, rx, delta, rate)
	}
}
