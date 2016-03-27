package planner

import (
	u "github.com/araddon/gou"
	"github.com/lytics/grid"

	//"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/plan"
)

var (
	_ exec.Task = (*SourceNats)(nil)
)

// A SinkNats task that receives messages that optionally may have been
//   hashed to be sent via nats to a nats source consumer.
//
//   taska-1 ->  hash-key -> nats-sink--> \                 / --> nats-source -->
//                                         \               /
//                                          --> gnatsd  -->
//                                         /               \
//   taska-2 ->  hash-key -> nats-sink--> /                 \ --> nats-source -->
//
type SinkNats struct {
	*exec.TaskBase
	tx          grid.Sender
	destination string
}

// New nats sink
func NewSinkNats(ctx *plan.Context, destination string, tx grid.Sender) *SinkNats {
	return &SinkNats{
		TaskBase:    exec.NewTaskBase(ctx),
		tx:          tx,
		destination: destination,
	}
}
func (m *SinkNats) Close() error {
	//inCh := m.MessageIn()
	//u.Debugf("SinkNats close")
	return nil
}
func (m *SinkNats) CloseFinal() error {
	defer func() {
		if r := recover(); r != nil {
			u.Warnf("error on close %v", r)
		}
	}()
	//close(inCh) we don't close input channels, upstream does
	//m.Ctx.Recover()
	m.tx.Close()
	return nil
}

//func (m *SinkNats) Close() error { return m.TaskBase.Close() }
func (m *SinkNats) Run() error {

	inCh := m.MessageIn()

	defer func() {
		//close(inCh) we don't close input channels, upstream does
		m.Ctx.Recover()
		//m.tx.Close()
	}()

	for {

		select {
		case <-m.SigChan():
			u.Debugf("got signal quit")
			return nil
		case msg, ok := <-inCh:
			if !ok {
				//u.Debugf("NICE, got msg shutdown")
				// eofMsg := datasource.NewSqlDriverMessageMapEmpty()
				// if err := m.tx.Send(m.destination, eofMsg); err != nil {
				// 	u.Errorf("Could not send eof message? %v", err)
				// 	return err
				// }
				return nil
			}

			//u.Debugf("In SinkNats topic:%q    msg:%#v", m.destination, msg)
			if err := m.tx.Send(m.destination, msg); err != nil {
				u.Errorf("Could not send message? %v", err)
				return err
			}
		}
	}
	return nil
}
