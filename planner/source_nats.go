package planner

import (
	"fmt"

	u "github.com/araddon/gou"
	"github.com/lytics/grid/grid.v2"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/plan"
)

var (
	_ exec.Task = (*SourceNats)(nil)
)

type CmdMsg struct {
	Cmd      string
	BodyJson u.JsonHelper
}

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
	closed  bool
	drainCt int
	cmdch   chan *CmdMsg
	rx      grid.Receiver
}

// Nats Source, the plan already provided info to the nats listener
// about which key/topic to listen to, Planner holds routing info not here.
func NewSourceNats(ctx *plan.Context, rx grid.Receiver) *SourceNats {
	return &SourceNats{
		TaskBase: exec.NewTaskBase(ctx),
		rx:       rx,
		cmdch:    make(chan *CmdMsg),
	}
}

// Close cleans up and closes channels
func (m *SourceNats) Close() error {
	//u.Debugf("SourceNats Close  alreadyclosed?%v", m.closed)
	//u.WarnT(8)
	if m.closed {
		return nil
	}
	m.closed = true

	defer func() {
		if r := recover(); r != nil {
			u.Warnf("error on close %v", r)
		}
	}()
	go m.drain()

	close(m.SigChan())

	//m.rx.Close()
	//return m.TaskBase.Close()
	return nil
}
func (m *SourceNats) drain() {

	for {
		select {
		case _, ok := <-m.rx.Msgs():
			if !ok {
				u.Debugf("%p NICE, got drain shutdown, drained %d msgs", m, m.drainCt)
				return
			}
			m.drainCt++
		}
	}
}

// CloseFinal after exit, cleanup some more
func (m *SourceNats) CloseFinal() error {
	//u.Debugf("SourceNats CloseFinal  alreadyclosed?%v", m.closed)
	defer func() {
		if r := recover(); r != nil {
			u.Warnf("error on close %v", r)
		}
	}()
	//close(inCh) we don't close input channels, upstream does
	//m.Ctx.Recover()
	m.rx.Close()
	close(m.cmdch)
	//return nil
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
		//u.Infof("%p defer SourceNats Run() exit", m)
		close(outCh)
		m.rx.Close()
	}()

	for {

		select {
		case <-quit:
			u.Warn("quit")
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
			if m.closed {
				// We want to drain this topic of nats,
				// not let it back up in gnatsd
				//u.Debugf("dropping message %v", msg)
				m.drainCt++
				continue
			}

			//u.Debugf("%p In SourceNats msg ", m)
			switch mt := msg.(type) {
			// case *datasource.SqlDriverMessageMap:
			// 	if len(mt.Vals) == 0 {
			// 		u.Infof("NICE EMPTY EOF MESSAGE, CLOSING")
			// 		return nil
			// 	}
			// 	outCh <- mt
			case datasource.SqlDriverMessageMap:
				if len(mt.Vals) == 0 {
					u.Infof("NICE EMPTY EOF MESSAGE 2")
					return nil
				}
				outCh <- &mt
			case *CmdMsg:
				u.Debugf("%p got cmdmsg  %#v", m, mt)
				m.cmdch <- mt
			case nil:
				//u.Debugf("%p got nil, assume this is a shutdown signal?", m)
				return nil
			default:
				u.Warnf("hm   %#v", mt)
				return fmt.Errorf("To use SourceNats must use SqlDriverMessageMap but got %T", msg)
			}
		}
	}
	return nil
}
