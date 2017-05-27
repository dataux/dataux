package planner

import (
	"bytes"
	"database/sql/driver"
	"encoding/gob"
	"io/ioutil"
	"time"

	u "github.com/araddon/gou"
	"github.com/lytics/grid/grid.v3"

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

	// Register our Message Types.
	grid.Register(Message{})
	grid.Register(SqlTask{})
	grid.Register(TaskResponse{})
}

// SinkSend is func to mock the Grid Client Request
type SinkSend func(msg interface{}) (interface{}, error)

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
	send        SinkSend
	destination string
}

// NewSink grid sink to route messages via gnatsd
func NewSink(ctx *plan.Context, destination string, send SinkSend) *Sink {
	return &Sink{
		TaskBase:    exec.NewTaskBase(ctx),
		send:        send,
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
	return m.TaskBase.Close()
}

// CloseFinal after shutdown cleanup the rest of channels
func (m *Sink) CloseFinal() error {
	defer func() {
		if r := recover(); r != nil {
			u.Warnf("error on close %v", r)
		}
	}()
	return nil
}

// Run blocking runner
func (m *Sink) Run() error {

	inCh := m.MessageIn()

	defer func() {
		//close(inCh) we don't close input channels, upstream does
		m.Ctx.Recover()
	}()

	buf := &bytes.Buffer{}

	for {

		select {
		case <-m.SigChan():
			u.Infof("got signal quit")
			return nil
		case msg, ok := <-inCh:
			if !ok {
				//u.Debugf("NICE, got msg shutdown")
				// eofMsg := datasource.NewSqlDriverMessageMapEmpty()
				// if err := m.tx.Send(m.destination, eofMsg); err != nil {
				// 	u.Errorf("Could not send eof message? %v", err)
				// 	return err
				// }
				_, err := m.send(&Message{})
				if err != nil {
					u.Warnf("could not send shutdown %v", err)
				}
				return nil
			}

			switch msg := msg.(type) {
			case *datasource.SqlDriverMessageMap:

				if msg == nil {
					u.Warnf("nil message, shutdown")
					return nil
				}
				enc := gob.NewEncoder(buf)
				err := enc.Encode(msg)
				if err != nil {
					u.Warnf("could not encode message %v", err)
					continue
				}
				//u.Debugf("err=%v , %s", err, buf.String())
				//u.Debugf("In Sink topic:%q    msg:%#v", m.destination, msg)
				by, err := ioutil.ReadAll(buf)
				if err != nil {
					u.Warnf("could not read message %v", err)
					continue
				}
				sm := Message{Msg: by}
				_, err = m.send(&sm)
				if err != nil {
					u.Warnf("mailbox: %v  error %v", m.destination, err)
					// Currently we shut down receiving nats listener, and this times-out
					if m.closed {
						return nil
					}
					u.Errorf("Could not send message? %v %T  %#v", err, msg, msg)
					return err
				}
				//u.Debugf("sent %v", res)
			default:
				u.Warnf("unhandled type %T", msg)
			}

		}
	}
	return nil
}
