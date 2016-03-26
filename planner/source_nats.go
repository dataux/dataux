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

// A SourceNats task that receives messages that optionally may have been
//   hash routed to this node.
//
//   taska-1 ->  hash-nats-sink  \                        / --> nats-source -->
//                                \                      /
//                                 --nats-route-by-key-->   --> nats-source -->
//                                /                      \
//   taska-2 ->  hash-nats-sink  /                        \ --> nats-source -->
//
type SourceNats struct {
	*exec.TaskBase
	rx grid.Receiver
}

// Nats Source, the plan already provided info to the nats listener
// about which key/topic to listen to, Planner holds routing info not here.
func NewSourceNats(ctx *plan.Context, rx grid.Receiver) *SourceNats {
	return &SourceNats{
		TaskBase: exec.NewTaskBase(ctx),
		rx:       rx,
	}
}

func (m *SourceNats) Close() error {
	//u.Debugf("SourceNats Close")
	return nil
}
func (m *SourceNats) CloseFinal() error {
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
func (m *SourceNats) Run() error {

	outCh := m.MessageOut()

	defer func() {
		m.Ctx.Recover()
		u.Infof("closing SourceNats Out")
		close(outCh)
		m.rx.Close()
	}()

	for {

		select {
		case <-m.SigChan():
			u.Debugf("got signal quit")
			return nil
		case msg, ok := <-m.rx.Msgs():
			if !ok {
				u.Debugf("NICE, got msg shutdown")
				return nil
			}

			//u.Debugf("In SourceNats msg %#v", msg)
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
				u.Debugf("got nil, assume this is a shutdown signal?")
				return nil
			default:
				u.Warnf("hm   %#v", mt)
				return fmt.Errorf("To use SourceNats must use SqlDriverMessageMap but got %T", msg)
			}
		}
	}
	return nil
}
