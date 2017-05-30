package planner

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
	// Our Source is meant to be inserted into a SQL dag
	// ensure it meets that interface
	_ exec.Task = (*Source)(nil)
)

// Source task is injected into a SQL dag task pipeline in order
// to recieve messages from another server via Grid Mailbox, for distribution
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
	sinkCt  int
	name    string
	c       <-chan grid.Request
}

// Source, the plan already provided info to the nats listener
// about which key/topic to listen to, Planner holds routing info not here.
func NewSource(ctx *plan.Context, mboxid string, c <-chan grid.Request) *Source {
	return &Source{
		TaskBase: exec.NewTaskBase(ctx),
		c:        c,
		name:     mboxid,
		sinkCt:   1,
	}
}

func (m *Source) MailboxId() string {
	return m.name
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

	return nil
}

func (m *Source) Setup(depth int) error {
	return m.TaskBase.Setup(depth)
}

func (m *Source) drain() {

	for {
		select {
		case _, ok := <-m.c:
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
	return m.TaskBase.Close()
}

func (m *Source) sinkStopped() bool {
	m.sinkCt--
	if m.sinkCt < 1 {
		return true
	}
	return false
}

// Run blocking runner
func (m *Source) Run() error {

	outCh := m.MessageOut()
	hasQuit := false
	quit := make(chan bool)
	actorCt := m.sinkCt

	defer func() {
		u.Infof("starting shutdown of Source")
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

	for {

		select {
		case <-quit:
			u.Debugf("Source got quit")
			return nil
		case <-m.SigChan():
			u.Debugf("%p got signal quit", m)
			return nil
		case req, ok := <-m.c:
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
			case *Message:

				// Respond to caller with empty message effectively acking message
				req.Respond(&Message{})

				if len(mt.Msg) == 0 {
					if m.sinkStopped() {
						u.Infof("NICE LAST EMPTY EOF MESSAGE 2")
						return nil
					} else {
						u.Infof("Got empty message but not last? %d:%v", actorCt, m.sinkCt)
						continue
					}
				}

				dec := gob.NewDecoder(buf)

				// These ones are special gob-encoded messages
				sm := datasource.SqlDriverMessageMap{}
				//u.Debugf("About to write %s", string(mt.Msg))
				buf.Write(mt.Msg)
				err := dec.Decode(&sm)
				buf.Reset()
				if err != nil {
					u.Warnf("error on read %v", err)
					continue
				}
				//u.Debugf("got msg %#v", sm)
				outCh <- &sm

			default:
				u.Warnf("hm   %#v", mt)
				return fmt.Errorf("To use Source must use SqlDriverMessageMap but got %T", req)
			}
		}
	}
	return nil
}
