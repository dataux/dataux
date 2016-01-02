package main

import (
	"time"

	u "github.com/araddon/gou"

	"github.com/lytics/dfa"
	"github.com/lytics/grid/grid2"
	"github.com/lytics/grid/grid2/condition"
	"github.com/lytics/grid/grid2/ring"
)

func NewConsumerActor(def *grid2.ActorDef, conf *Conf) grid2.Actor {
	return &ConsumerActor{
		def:   def,
		conf:  conf,
		flow:  Flow(def.Settings["flow"]),
		state: NewConsumerState(),
	}
}

type ConsumerActor struct {
	def      *grid2.ActorDef
	conf     *Conf
	flow     Flow
	grid     grid2.Grid
	tx       grid2.Sender
	rx       grid2.Receiver
	exit     <-chan bool
	started  condition.Join
	finished condition.Join
	state    *ConsumerState
	chaos    *Chaos
}

func (a *ConsumerActor) ID() string {
	return a.def.ID()
}

func (a *ConsumerActor) String() string {
	return a.ID()
}

func (a *ConsumerActor) Act(g grid2.Grid, exit <-chan bool) bool {
	tx, err := grid2.NewSender(g.Nats(), 100)
	if err != nil {
		u.Fatalf("%v: error: %v", a.ID(), err)
	}
	defer tx.Close()

	rx, err := grid2.NewReceiver(g.Nats(), a.ID(), 4, 0)
	if err != nil {
		u.Fatalf("%v: error: %v", a.ID(), err)
	}
	defer rx.Close()

	a.tx = tx
	a.rx = rx
	a.grid = g
	a.exit = exit
	a.chaos = NewChaos(a.ID())
	defer a.chaos.Stop()

	d := dfa.New()
	d.SetStartState(Starting)
	d.SetTerminalStates(Exiting, Terminating)
	d.SetTransitionLogger(func(state dfa.State) {
		//u.Debugf("%v: switched to state: %v", a, state)
	})

	d.SetTransition(Starting, EverybodyStarted, Running, a.Running)
	d.SetTransition(Starting, EverybodyFinished, Terminating, a.Terminating)
	d.SetTransition(Starting, Failure, Exiting, a.Exiting)
	d.SetTransition(Starting, Exit, Exiting, a.Exiting)

	d.SetTransition(Running, SendFailure, Resending, a.Resending)
	d.SetTransition(Running, EverybodyFinished, Finishing, a.Finishing)
	d.SetTransition(Running, Failure, Exiting, a.Exiting)
	d.SetTransition(Running, Exit, Exiting, a.Exiting)

	d.SetTransition(Resending, SendSuccess, Running, a.Running)
	d.SetTransition(Resending, SendFailure, Resending, a.Resending)
	d.SetTransition(Resending, Failure, Exiting, a.Exiting)
	d.SetTransition(Resending, Exit, Exiting, a.Exiting)

	d.SetTransition(Finishing, EverybodyFinished, Terminating, a.Terminating)
	d.SetTransition(Finishing, Failure, Exiting, a.Exiting)
	d.SetTransition(Finishing, Exit, Exiting, a.Exiting)

	final, err := d.Run(a.Starting)
	//u.Warnf("%s consumer final: %s", a, final.String())
	if err != nil {
		u.Fatalf("%v: error: %v", a, err)
	}
	if final == Terminating {
		return true
	}
	return false
}

func (a *ConsumerActor) Starting() dfa.Letter {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	// Why are we sleeping, what are we waiting for?
	time.Sleep(3 * time.Second)

	j := condition.NewJoin(a.grid.Etcd(), 2*time.Minute, a.grid.Name(), a.flow.Name(), "started", a.ID())
	if err := j.Rejoin(); err != nil {
		return Failure
	}
	a.started = j

	w := condition.NewCountWatch(a.grid.Etcd(), a.grid.Name(), a.flow.Name(), "started")
	defer w.Stop()

	f := condition.NewNameWatch(a.grid.Etcd(), a.grid.Name(), a.flow.Name(), "finished")
	defer f.Stop()

	started := w.WatchUntil(a.conf.NrConsumers + a.conf.NrProducers + 1)
	finished := f.WatchUntil(a.flow.NewContextualName("leader"))
	for {
		select {
		case <-a.exit:
			return Exit
		case <-a.chaos.C:
			return Failure
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

func (a *ConsumerActor) Finishing() dfa.Letter {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	j := condition.NewJoin(a.grid.Etcd(), 10*time.Minute, a.grid.Name(), a.flow.Name(), "finished", a.ID())
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
		case <-a.chaos.C:
			return Failure
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

func (a *ConsumerActor) Running() dfa.Letter {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	w := condition.NewNameWatch(a.grid.Etcd(), a.grid.Name(), a.flow.Name(), "finished")
	defer w.Stop()

	n := 0
	finished := w.WatchUntil(ring.New(a.flow.NewContextualName("producer"), a.conf.NrProducers))
	for {
		select {
		case <-a.exit:
			return Exit
		case <-a.chaos.C:
			return Failure
		case <-ticker.C:
			if err := a.started.Alive(); err != nil {
				return Failure
			}
		case <-finished:
			if err := a.SendCounts(); err != nil {
				return SendFailure
			} else {
				return EverybodyFinished
			}
		case err := <-w.WatchError():
			u.Warnf("%v: error: %v", a, err)
			return Failure
		case m := <-a.rx.Msgs():
			switch m := m.(type) {
			case DataMsg:
				a.state.Counts[m.Producer]++
				n++
				if n%1000000 == 0 {
					u.Debugf("%v: received: %v", a.ID(), n)
				}
				if n%1000 == 0 {
					if err := a.SendCounts(); err != nil {
						return SendFailure
					}
				}
			}
		}
	}
}

func (a *ConsumerActor) Resending() dfa.Letter {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-a.exit:
			return Exit
		case <-a.chaos.C:
			return Failure
		case <-ticker.C:
			if err := a.started.Alive(); err != nil {
				u.Errorf("%v: failed to report 'started' liveness, but ignoring to flush send buffers", a)
			}
			if err := a.SendCounts(); err == nil {
				return SendSuccess
			}
		}
	}
}

func (a *ConsumerActor) Exiting() {
	if a.started != nil {
		a.started.Stop()
	}
	if a.finished != nil {
		a.finished.Stop()
	}

	if err := a.SendCounts(); err != nil {
		u.Warnf("%v: failed to flush send buffers, trying again", a)
		time.Sleep(5 * time.Second)
		if err := a.SendCounts(); err != nil {
			u.Warnf("%v: failed to flush send buffers, data is being dropped", a)
		}
	}
}

func (a *ConsumerActor) Terminating() {
	if a.started != nil {
		a.started.Stop()
	}
	if a.finished != nil {
		a.finished.Stop()
	}
}

func (a *ConsumerActor) SendCounts() error {
	for p, n := range a.state.Counts {
		if err := a.tx.Send(a.flow.NewContextualName("leader"), &ResultMsg{Producer: p, Count: n, From: a.ID()}); err != nil {
			return err
		} else {
			delete(a.state.Counts, p)
		}
	}
	return nil
}

type ConsumerState struct {
	Counts map[string]int
}

func NewConsumerState() *ConsumerState {
	return &ConsumerState{Counts: make(map[string]int)}
}
