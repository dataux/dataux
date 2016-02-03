package planner

import (
	"fmt"

	u "github.com/araddon/gou"
	"github.com/lytics/grid"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/plan"
)

var _ = u.EMPTY

// Nats Channel Source
type SourceNats struct {
	*exec.TaskBase
	rx grid.Receiver
}

// A SourceNats task that receives messages that optionally may have been
//   hash routed to this node.
//
//   taska-1 ->  hash-key  \                / --> nats-source -->
//                          \              /
//                           --nats-sink-->   --> nats-source -->
//                          /              \
//   taska-2 ->  hash-key  /                \ --> nats-source -->
//
func NewSourceNats(ctx *plan.Context, rx grid.Receiver) *SourceNats {
	return &SourceNats{
		TaskBase: exec.NewTaskBase(ctx),
		rx:       rx,
	}
}

func (m *SourceNats) Copy() *SourceNats { return &SourceNats{} }
func (m *SourceNats) Close() error      { return m.TaskBase.Close() }

func (m *SourceNats) Run() error {

	outCh := m.MessageOut()

	defer func() {
		m.Ctx.Recover()
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

			u.Infof("In SourceNats msg %#v", msg)
			switch mt := msg.(type) {
			case *datasource.SqlDriverMessageMap:
				if len(mt.Vals) == 0 {
					u.Infof("NICE EMPTY EOF MESSAGE")
					return nil
				}
				outCh <- mt
			case datasource.SqlDriverMessageMap:
				if len(mt.Vals) == 0 {
					u.Infof("NICE EMPTY EOF MESSAGE")
					return nil
				}
				outCh <- &mt
			default:
				u.Warnf("hm   %#v", mt)
				return fmt.Errorf("To use SourceNats must use SqlDriverMessageMap but got %T", msg)
			}
		}
	}
	return nil
}
