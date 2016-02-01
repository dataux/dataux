package gridrunner

import (
	"encoding/gob"
	"time"

	u "github.com/araddon/gou"

	"github.com/lytics/dfa"
	"github.com/lytics/grid"
	"github.com/lytics/grid/condition"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/plan"
)

var (
	// TEMP HACK
	// t plan.Task, sp *plan.Select
	tempTask       plan.Task
	tempSelectPlan *plan.Select
)

func init() {
	gob.Register(datasource.SqlDriverMessageMap{})
	//gob.Register(DataMsg{})
}

func NewLeaderActor(def *grid.ActorDef, conf *Conf) grid.Actor {
	return &LeaderActor{
		def:  def,
		conf: conf,
		flow: Flow(def.Settings["flow"]),
	}
}

type LeaderActor struct {
	def      *grid.ActorDef
	conf     *Conf
	flow     Flow
	grid     grid.Grid
	rx       grid.Receiver
	exit     <-chan bool
	started  condition.Join
	finished condition.Join
	state    *LeaderState
}

func (a *LeaderActor) ID() string {
	return a.def.ID()
}

func (a *LeaderActor) String() string {
	return a.ID()
}

func (a *LeaderActor) Act(g grid.Grid, exit <-chan bool) bool {
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

func (a *LeaderActor) Starting() dfa.Letter {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	//time.Sleep(3 * time.Second)

	u.Infof("starting actor %#v", a.def)

	j := condition.NewJoin(a.grid.Etcd(), 2*time.Minute, a.grid.Name(), a.flow.Name(), "started", a.ID())
	if err := j.Rejoin(); err != nil {
		return Failure
	}
	a.started = j

	w := condition.NewCountWatch(a.grid.Etcd(), a.grid.Name(), a.flow.Name(), "started")
	defer w.Stop()

	f := condition.NewNameWatch(a.grid.Etcd(), a.grid.Name(), a.flow.Name(), "finished")
	defer f.Stop()

	//started := w.WatchUntil(a.conf.NrConsumers + a.conf.NrProducers + 1)
	started := w.WatchUntil(1)
	finished := f.WatchUntil(a.flow.NewContextualName("leader"))
	for {
		select {
		case <-a.exit:
			return Exit
		case <-ticker.C:
			if err := a.started.Alive(); err != nil {
				return Failure
			}
		case <-started:
			return EverybodyStarted
		case <-finished:
			return EverybodyFinished
		}
	}
}

func (a *LeaderActor) RunSqlDag() {

	taskRunner, ok := tempTask.(exec.TaskRunner)
	if !ok {
		u.Errorf("Expected exec.TaskRunner but got %T", tempTask)
		return
	}
	taskRunner.Setup(0)
	err := taskRunner.Run()
	if err != nil {
		u.Errorf("error on Query.Run(): %v", err)
	}
	taskRunner.Close()
	j := condition.NewJoin(a.grid.Etcd(), 10*time.Second, a.grid.Name(), a.flow.Name(), "sqlcomplete", a.ID())
	u.Infof("sending sqlcomplete join message for %#v", taskRunner)
	if err = j.Rejoin(); err != nil {
		u.Errorf("could not join?? %v", err)
	}
	u.Warnf("sqlcomplete sql dag")
}

func (a *LeaderActor) Finishing() dfa.Letter {

	u.Warnf("%s leader finishing", a.String())
	return EverybodyFinished

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	j := condition.NewJoin(a.grid.Etcd(), 10*time.Minute, a.grid.Name(), a.flow.Name(), "finished")
	if err := j.Rejoin(); err != nil {
		return Failure
	}
	a.finished = j

	w := condition.NewCountWatch(a.grid.Etcd(), a.grid.Name(), a.flow.Name(), "finished")
	defer w.Stop()

	finished := w.WatchUntil(a.conf.NrConsumers + a.conf.NrProducers + 1)
	for {
		select {
		case <-a.exit:
			return Exit
		case <-ticker.C:
			if err := a.started.Alive(); err != nil {
				return Failure
			}
			if err := a.finished.Alive(); err != nil {
				return Failure
			}
		case <-finished:
			a.started.Exit()
			a.finished.Alive()
			return EverybodyFinished
		case err := <-w.WatchError():
			u.Errorf("%v: error: %v", a, err)
			return Failure
		}
	}
}

func (a *LeaderActor) Running() dfa.Letter {

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	a.state = NewLeaderState()
	s := condition.NewState(a.grid.Etcd(), 10*time.Minute, a.grid.Name(), a.flow.Name(), "state", a.ID())
	defer s.Stop()

	if err := s.Init(a.state); err != nil {
		if _, err := s.Fetch(a.state); err != nil {
			return FetchStateFailure
		}
	}
	u.Infof("%v: running with state: %v, index: %v", a.ID(), a.state, s.Index())

	w := condition.NewCountWatch(a.grid.Etcd(), a.grid.Name(), a.flow.Name(), "sqlcomplete")
	defer w.Stop()

	// Now run the Actual worker
	go a.RunSqlDag()

	finished := w.WatchUntil(1)
	for {
		select {
		case <-a.exit:
			if _, err := s.Store(a.state); err != nil {
				u.Warnf("%v: failed to save state: %v", a, err)
			}
			u.Warnf("%s finished store", a)
			return Exit
		case <-ticker.C:
			u.Infof("alive")
			if err := a.started.Alive(); err != nil {
				u.Warnf("leader not alive?: %v", err)
				return Failure
			}
			u.Warnf("%s about to do ticker store", a)
			if _, err := s.Store(a.state); err != nil {
				u.Warnf("could not store: %v", err)
				return Failure
			}
		case <-finished:
			u.Warnf("%s leader about to send finished signal?", a)
			if _, err := s.Store(a.state); err != nil {
				u.Warnf("%v: failed to save state: %v", a, err)
			}
			return EverybodyFinished
		case err := <-w.WatchError():
			u.Errorf("%v: error: %v", a, err)
			return Failure
		case m := <-a.rx.Msgs():

			// Like any actor we can recieve normal messages
			// in this situation we have actors report back metrics to us
			// for this sample, we have some count of messages in state, but normally
			// you would have progress (kafkaId's, offsetids, status) in state
			// and metrics here
			u.Debugf("%s rx Msg:  %#v", a, m)
			// switch m := m.(type) {
			// case ResultMsg:

			// }
		}
	}
}

func (a *LeaderActor) Exiting() {
	if a.started != nil {
		a.started.Stop()
	}
	if a.finished != nil {
		a.finished.Stop()
	}
}

func (a *LeaderActor) Terminating() {
	u.Warnf("%s leader terminating", a)
	if a.started != nil {
		a.started.Stop()
	}
	if a.finished != nil {
		a.finished.Stop()
	}

	if a.state == nil {
		u.Warnf("%s nil state?", a)
		return
	}

	for p, n := range a.state.ProducerCounts {
		rx := a.state.ConsumerCounts[p]
		delta := a.state.ConsumerCounts[p] - n
		rate := float64(a.state.ProducerCounts[p]) / a.state.ProducerDurations[p]
		u.Infof("%v: producer: %v, sent: %v, consumers received: %v, delta: %v, rate: %2.f/s", a.ID(), p, n, rx, delta, rate)
	}
}

type LeaderState struct {
	Start             time.Time
	ConsumerCounts    map[string]int
	ProducerCounts    map[string]int
	ProducerDurations map[string]float64
}

func NewLeaderState() *LeaderState {
	return &LeaderState{ConsumerCounts: make(map[string]int), ProducerCounts: make(map[string]int), ProducerDurations: make(map[string]float64)}
}
