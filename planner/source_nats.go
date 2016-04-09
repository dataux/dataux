package planner

import (
	"fmt"

	u "github.com/araddon/gou"
	"github.com/lytics/grid"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/plan"
)

var (
	_ exec.Task = (*SourceNats)(nil)
)

// SourceNats task that receives messages via Gnatsd, for distribution
//  across multiple workers.  These messages optionally may have been
//   hash routed to this node, ie partition-key routed.
//
//   taska-1 ->  hash-nats-sink  \                        / --> nats-source -->
//                                \                      /
//                                 --nats-route-by-key-->   --> nats-source -->
//                                /                      \
//   taska-2 ->  hash-nats-sink  /                        \ --> nats-source -->
//
type SourceNats struct {
	*exec.TaskBase
	closed bool
	rx     grid.Receiver
}

// Nats Source, the plan already provided info to the nats listener
// about which key/topic to listen to, Planner holds routing info not here.
func NewSourceNats(ctx *plan.Context, rx grid.Receiver) *SourceNats {
	return &SourceNats{
		TaskBase: exec.NewTaskBase(ctx),
		rx:       rx,
	}
}

// Close cleans up and closes channels
func (m *SourceNats) Close() error {
	u.Debugf("SourceNats Close  alreadyclosed?%v", m.closed)
	if m.closed {
		return nil
	}
	m.closed = true

	defer func() {
		if r := recover(); r != nil {
			u.Warnf("error on close %v", r)
		}
	}()

	m.rx.Close()
	return m.TaskBase.Close()
	//return nil
}

// CloseFinal after exit, cleanup some more
func (m *SourceNats) CloseFinal() error {
	u.Debugf("SourceNats CloseFinal  alreadyclosed?%v", m.closed)
	defer func() {
		if r := recover(); r != nil {
			u.Warnf("error on close %v", r)
		}
	}()
	//close(inCh) we don't close input channels, upstream does
	//m.Ctx.Recover()
	m.rx.Close()
	return m.TaskBase.Close()
}

// Run a blocking runner
func (m *SourceNats) Run() error {

	outCh := m.MessageOut()
	hasQuit := false
	quit := make(chan bool)

	defer func() {
		//m.Ctx.Recover()
		close(quit)
		hasQuit = true
		// We are going to hit panic on closed channels?
		defer func() {
			if r := recover(); r != nil {
				u.Warnf("error on defer/exit nats source %v", r)
			}
		}()
		u.Infof("%p defer SourceNats Run() exit", m)
		close(outCh)
		m.rx.Close()
	}()

	for {

		select {
		case <-quit:
			return nil
		case <-m.SigChan():
			u.Debugf("%p got signal quit", m)
			return nil
		case msg, ok := <-m.rx.Msgs():
			if !ok {
				u.Debugf("%p NICE, got msg shutdown", m)
				return nil
			}
			if hasQuit {
				u.Debugf("%p NICE, already quit!", m)
				return nil
			}

			//u.Debugf("%p In SourceNats msg ", m)
			switch mt := msg.(type) {
			case *datasource.SqlDriverMessageMap:
				if len(mt.Vals) == 0 {
					u.Infof("NICE EMPTY EOF MESSAGE, CLOSING")
					return nil
				}
				outCh <- mt
			case datasource.SqlDriverMessageMap:
				if len(mt.Vals) == 0 {
					u.Infof("NICE EMPTY EOF MESSAGE 2")
					return nil
				}
				outCh <- &mt
			case nil:
				u.Debugf("%p got nil, assume this is a shutdown signal?", m)
				return nil
			default:
				u.Warnf("hm   %#v", mt)
				return fmt.Errorf("To use SourceNats must use SqlDriverMessageMap but got %T", msg)
			}
		}
	}
	return nil
}
