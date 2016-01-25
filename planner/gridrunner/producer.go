package gridrunner

import (
	"time"

	u "github.com/araddon/gou"

	"github.com/lytics/dfa"
	"github.com/lytics/grid/grid2"
	"github.com/lytics/grid/grid2/condition"
	"github.com/lytics/grid/grid2/ring"
)

type ProducerState struct {
	SentMessages int     `json:"sent_messages"`
	Duration     float64 `json:"send_messages"`
}

func NewProducerState() *ProducerState {
	return &ProducerState{SentMessages: 0, Duration: 0}
}

// Our producer
type ProducerActor struct {
	def      *grid2.ActorDef
	conf     *Conf
	flow     Flow
	grid     grid2.Grid
	tx       grid2.Sender
	exit     <-chan bool
	started  condition.Join
	finished condition.Join
	state    *ProducerState
}

func NewProducerActor(def *grid2.ActorDef, conf *Conf) grid2.Actor {
	return &ProducerActor{
		def:  def,
		conf: conf,
		flow: Flow(def.Settings["flow"]),
	}
}

func (a *ProducerActor) ID() string {
	return a.def.ID()
}

func (a *ProducerActor) String() string {
	return a.ID()
}

func (a *ProducerActor) Act(g grid2.Grid, exit <-chan bool) bool {
	tx, err := grid2.NewSender(g.Nats(), 100)
	if err != nil {
		u.Errorf("%v: error: %v", a.ID(), err)
	}
	defer tx.Close()

	a.tx = tx
	a.grid = g
	a.exit = exit

	// Now, we are going to define all of the State's, transitions
	// for the StateMachine lifecycle of this actor
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

	d.SetTransition(Running, SendFailure, Resending, a.Resending)
	d.SetTransition(Running, IndividualFinished, Finishing, a.Finishing)
	d.SetTransition(Running, Failure, Exiting, a.Exiting)
	d.SetTransition(Running, Exit, Exiting, a.Exiting)

	d.SetTransition(Resending, SendSuccess, Running, a.Running)
	d.SetTransition(Resending, SendFailure, Resending, a.Resending)
	d.SetTransition(Resending, Failure, Exiting, a.Exiting)
	d.SetTransition(Resending, Exit, Exiting, a.Exiting)

	d.SetTransition(Finishing, EverybodyFinished, Terminating, a.Terminating)
	d.SetTransition(Finishing, Failure, Exiting, a.Exiting)
	d.SetTransition(Finishing, Exit, Exiting, a.Exiting)

	// This call is blocking
	final, err := d.Run(a.Starting)
	//u.Warnf("%s producer final: %s", a, final.String())
	if err != nil {
		u.Errorf("%v: error: %v", a, err)
	}
	if final == Terminating {
		return true
	}
	return false
}

// Starting is the process of getting everything setup and waiting
//  for other actors in this workflow to be started as well before we run.
func (a *ProducerActor) Starting() dfa.Letter {

	u.Debugf("%s starting", a)

	// Create a hearbeat ticket to see if we are still alive
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	//time.Sleep(3 * time.Second)

	// Create a join condition to see that
	//  other actors are joining in this workflow
	j := condition.NewJoin(a.grid.Etcd(), 2*time.Minute, a.grid.Name(), a.flow.Name(), "started", a.ID())
	if err := j.Rejoin(); err != nil {
		return Failure
	}
	a.started = j

	// For starting we want to ensure that appropriate number of
	//  other producer/consumers have joined this rendezvous
	w := condition.NewCountWatch(a.grid.Etcd(), a.grid.Name(), a.flow.Name(), "started")
	defer w.Stop()
	started := w.WatchUntil(a.conf.NrConsumers + a.conf.NrProducers + 1)

	// For finish, we watch until the Leader has finished
	f := condition.NewNameWatch(a.grid.Etcd(), a.grid.Name(), a.flow.Name(), "finished")
	defer f.Stop()
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
			u.Debugf("%s starting complete", a)
			return EverybodyStarted
		case <-finished:
			return EverybodyFinished
		}
	}
}

func (a *ProducerActor) Finishing() dfa.Letter {

	u.Debugf("%s finishing", a)

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	j := condition.NewJoin(a.grid.Etcd(), 10*time.Minute, a.grid.Name(), a.flow.Name(), "finished", a.ID())
	if err := j.Rejoin(); err != nil {
		return Failure
	}
	a.finished = j

	w := condition.NewCountWatch(a.grid.Etcd(), a.grid.Name(), a.flow.Name(), "finished")
	defer w.Stop()

	finished := w.WatchUntil(a.conf.NrConsumers + a.conf.NrConsumers + 1)
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
			u.Warnf("%v: error: %v", a, err)
			return Failure
		}
	}
}

func (a *ProducerActor) Running() dfa.Letter {

	u.Debugf("%s running", a)

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	a.state = NewProducerState()
	s := condition.NewState(a.grid.Etcd(), 30*time.Minute, a.grid.Name(), a.flow.Name(), "state", a.ID())
	defer s.Stop()

	if err := s.Init(a.state); err != nil {
		if _, err := s.Fetch(a.state); err != nil {
			return FetchStateFailure
		}
	}
	//u.Debugf("%v: running with state: %v, index: %v", a.ID(), a.state, s.Index())

	// Make some random length string data.
	data := NewDataMaker(a.conf.MsgSize, a.conf.MsgCount-a.state.SentMessages)
	defer data.Stop()

	// Our ring will help us
	r := ring.New(a.flow.NewContextualName("consumer"), a.conf.NrConsumers)
	start := time.Now()
	for {
		select {
		case <-a.exit:
			if _, err := s.Store(a.state); err != nil {
				u.Warnf("%v: failed to save state: %v", a, err)
			}
			return Exit
		case <-ticker.C:
			if err := a.started.Alive(); err != nil {
				return Failure
			}
			if _, err := s.Store(a.state); err != nil {
				return Failure
			}
		case <-data.Done():
			if err := a.tx.Flush(); err != nil {
				return SendFailure
			}
			if err := a.tx.Send(a.flow.NewContextualName("leader"), &ResultMsg{Producer: a.ID(), Count: a.state.SentMessages, From: a.ID(), Duration: a.state.Duration}); err != nil {
				return SendFailure
			}
			if _, err := s.Store(a.state); err != nil {
				u.Warnf("%v: failed to save state: %v", a, err)
			}
			return IndividualFinished
		case d := <-data.Next():
			if a.state.SentMessages%100 == 0 {
				a.state.Duration += time.Now().Sub(start).Seconds()
				start = time.Now()
			}
			if a.state.SentMessages%10000 == 0 {
				if _, err := s.Store(a.state); err != nil {
					u.Warnf("%v: failed to save state: %v", a, err)
					return Failure
				}
			}

			// We create a partitionId (ie, which consumer gets it) randomly
			//  - choosing how to partition is important for certain types of tasks
			partitionId := r.ByInt(a.state.SentMessages)

			if err := a.tx.SendBuffered(partitionId, &DataMsg{Producer: a.ID(), Data: d}); err != nil {
				if _, err := s.Store(a.state); err != nil {
					u.Warnf("%v: failed to save state: %v", a, err)
				}
				return SendFailure
			}
			a.state.SentMessages++
		}
	}
}

func (a *ProducerActor) Resending() dfa.Letter {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	fastticker := time.NewTicker(5 * time.Second)
	defer fastticker.Stop()
	for {
		select {
		case <-a.exit:
			return Exit
		case <-ticker.C:
			if err := a.started.Alive(); err != nil {
				u.Warnf("%v: failed to report 'started' liveness, but ignoring to flush send buffers", a)
			}
		case <-fastticker.C:
			if err := a.tx.Flush(); err == nil {
				return SendSuccess
			}
		}
	}
}

func (a *ProducerActor) Exiting() {
	if a.started != nil {
		a.started.Stop()
	}
	if a.finished != nil {
		a.finished.Stop()
	}

	if err := a.tx.Flush(); err != nil {
		u.Warnf("%v: failed to flush send buffers, trying again", a)
		time.Sleep(5 * time.Second)
		if err := a.tx.Flush(); err != nil {
			u.Errorf("%v: failed to flush send buffers, data is being dropped", a)
		}
	}
}

func (a *ProducerActor) Terminating() {
	if a.started != nil {
		a.started.Stop()
	}
	if a.finished != nil {
		a.finished.Stop()
	}
}
