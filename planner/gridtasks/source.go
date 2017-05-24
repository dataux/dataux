package gridtasks

import (
	"bytes"
	"encoding/gob"
	"fmt"

	u "github.com/araddon/gou"
	"github.com/lytics/grid/grid.v3"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/plan"
)

var (
	_ exec.Task = (*Source)(nil)
)

type CmdMsg struct {
	Cmd      string
	BodyJson u.JsonHelper
}

// Source task that receives messages via Grid, for distribution
// across multiple workers.  These messages optionally may have been
// hash routed to this node, ie partition-key routed.
//
//   taska-1 ->  hash-sink  \                        / --> source -->
//                           \                      /
//                             --route-by-key-->   --> source -->
//                           /                      \
//   taska-2 ->  hash-sink  /                        \ --> source -->
//
type Source struct {
	*exec.TaskBase
	closed  bool
	drainCt int
	name    string
	cmdch   chan *CmdMsg
	server  *grid.Server
	mbox    *grid.Mailbox
}

// Source, the plan already provided info to the nats listener
// about which key/topic to listen to, Planner holds routing info not here.
func NewSource(ctx *plan.Context, s *grid.Server) *Source {
	return &Source{
		TaskBase: exec.NewTaskBase(ctx),
		server:   s,
		cmdch:    make(chan *CmdMsg),
	}
}

// Close cleans up and closes channels
func (m *Source) Close() error {
	//u.Debugf("Source Close  alreadyclosed?%v", m.closed)
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

func (m *Source) Setup(depth int) error {
	// Listen to a mailbox with the same
	// name as the actor.
	mailbox, err := grid.NewMailbox(m.server, m.name, 10)
	if err != nil {
		u.Errorf("could not read %v", err)
		return err
	}
	m.mbox = mailbox
	u.Infof("started mailbox %q", m.name)

	return m.TaskBase.Setup(depth)
}
func (m *Source) drain() {

	for {
		select {
		case _, ok := <-m.mbox.C:
			if !ok {
				u.Debugf("%p NICE, got drain shutdown, drained %d msgs", m, m.drainCt)
				return
			}
			m.drainCt++
		}
	}
}

// CloseFinal after exit, cleanup some more
func (m *Source) CloseFinal() error {
	//u.Debugf("Source CloseFinal  alreadyclosed?%v", m.closed)
	defer func() {
		if r := recover(); r != nil {
			u.Warnf("error on close %v", r)
		}
	}()
	if m.mbox != nil {
		m.mbox.Close()
	}
	close(m.cmdch)
	return m.TaskBase.Close()
}

// Run a blocking runner
func (m *Source) Run() error {

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
		//u.Infof("%p defer Source Run() exit", m)
		close(outCh)
	}()

	buf := &bytes.Buffer{}
	dec := gob.NewDecoder(buf)

	for {

		select {
		case <-quit:
			u.Warn("quit")
			return nil
		case <-m.SigChan():
			u.Debugf("%p got signal quit", m)
			return nil
		case req, ok := <-m.mbox.C:
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
				u.Debugf("dropping message %v", req)
				m.drainCt++
				continue
			}

			//u.Debugf("%p In Source msg %T", m, m)
			switch mt := req.Msg().(type) {
			// case *datasource.SqlDriverMessageMap:
			// 	if len(mt.Vals) == 0 {
			// 		u.Infof("NICE EMPTY EOF MESSAGE, CLOSING")
			// 		return nil
			// 	}
			// 	outCh <- mt
			case *Message:
				if len(mt.Msg) == 0 {
					u.Infof("NICE EMPTY EOF MESSAGE 2")
					return nil
				}
				sm := datasource.SqlDriverMessageMap{}
				buf.Write(mt.Msg)
				err := dec.Decode(&sm)
				if err != nil {
					u.Warnf("error on read %v", err)
				}
				if len(sm.Vals) == 0 {
					u.Infof("NICE EMPTY EOF MESSAGE 3")
					return nil
				}
				//u.Debugf("got msg %#v", sm)
				req.Respond(req.Msg())
				outCh <- &sm
			case datasource.SqlDriverMessageMap:
				if len(mt.Vals) == 0 {
					u.Infof("NICE EMPTY EOF MESSAGE 4")
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
				return fmt.Errorf("To use Source must use SqlDriverMessageMap but got %T", req)
			}
		}
	}
	return nil
}
