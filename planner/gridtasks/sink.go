package gridtasks

import (
	"database/sql/driver"
	"encoding/gob"
	"time"

	u "github.com/araddon/gou"
	"github.com/lytics/grid/grid.v2"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/plan"
)

var (
	_ exec.Task = (*Sink)(nil)
)

func init() {
	// Really not a good place for this
	gob.Register(map[string]interface{}{})
	gob.Register(time.Time{})
	gob.Register(datasource.SqlDriverMessageMap{})
	gob.Register([]driver.Value{})
	gob.Register(CmdMsg{})
}

// Sink task that receives messages that optionally may have been
// hashed to be sent via nats to a nats source consumer.
//
//   taska-1 ->  hash-key -> sink--> \                 / --> source -->
//                                    \               /
//                                      --> grid  -->
//                                    /               \
//   taska-2 ->  hash-key -> sink--> /                 \ --> source -->
//
type Sink struct {
	*exec.TaskBase
	closed      bool
	tx          grid.Sender
	destination string
}

// NewSink grid sink to route messages via gnatsd
func NewSink(ctx *plan.Context, destination string, tx grid.Sender) *Sink {
	return &Sink{
		TaskBase:    exec.NewTaskBase(ctx),
		tx:          tx,
		destination: destination,
	}
}

// Close cleanup and coalesce
func (m *Sink) Close() error {
	//u.Debugf("%p Sink Close()", m)
	if m.closed {
		return nil
	}
	m.closed = true
	m.tx.Close()
	return m.TaskBase.Close()
}

// CloseFinal after shutdown cleanup the rest of channels
func (m *Sink) CloseFinal() error {
	defer func() {
		if r := recover(); r != nil {
			u.Warnf("error on close %v", r)
		}
	}()
	m.tx.Close()
	return nil
}

// Run blocking runner
func (m *Sink) Run() error {

	inCh := m.MessageIn()

	defer func() {
		//close(inCh) we don't close input channels, upstream does
		m.Ctx.Recover()
		//m.tx.Close()
	}()

	for {

		select {
		case <-m.SigChan():
			//u.Debugf("got signal quit")
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

			//u.Debugf("In Sink topic:%q    msg:%#v", m.destination, msg)
			if err := m.tx.Send(m.destination, msg); err != nil {
				// Currently we shut down receiving nats listener, and this times-out
				if m.closed {
					return nil
				}
				u.Errorf("Could not send message? %v %T  %#v", err, msg, msg)
				return err
			}
		}
	}
	return nil
}
