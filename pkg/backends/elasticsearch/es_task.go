package elasticsearch

import (
	"database/sql/driver"
	"io"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/expr"
)

type EsTask struct {
	*exec.TaskBase
	Req *SqlToEs
}

func NewEstask(sqlToEs *SqlToEs) *EsTask {
	m := &EsTask{
		TaskBase: exec.NewTaskBase("EsTask"),
	}
	m.Handler = resultWrite(m)
	return m
}

func (m *EsTask) Copy() *EsTask { return nil }
func (m *EsTask) Close() error  { return nil }

// Note, this is implementation of the sql/driver Rows() Next() interface
func (m *EsTask) Next(dest []driver.Value) error {
	select {
	case <-m.SigChan():
		return exec.ShuttingDownError
	case err := <-m.ErrChan():
		return err
	case msg, ok := <-m.MessageIn():
		if !ok {
			return io.EOF
		}
		if msg == nil {
			u.Warnf("nil message?")
			return io.EOF
			//return fmt.Errorf("Nil message error?")
		}
		return msgToRow(msg, nil, dest)
	}
}

func resultWrite(m *EsTask) exec.MessageHandler {
	out := m.MessageOut()
	return func(ctx *exec.Context, msg datasource.Message) bool {

		// defer func() {
		// 	if r := recover(); r != nil {
		// 		u.Errorf("crap, %v", r)
		// 	}
		// }()
		if msgReader, ok := msg.Body().(expr.ContextReader); ok {
			u.Debugf("got msg in result writer: %#v", msgReader)
		} else {
			u.Errorf("could not convert to message reader: %T", msg.Body())
		}

		select {
		case out <- msg:
			return true
		case <-m.SigChan():
			return false
		}
	}
}

func msgToRow(msg datasource.Message, cols []string, dest []driver.Value) error {
	// defer func() {
	// 	if r := recover(); r != nil {
	// 		u.Errorf("crap, %v", r)
	// 	}
	// }()
	//u.Debugf("msg? %v  %T \n%p %v", msg, msg, dest, dest)
	switch mt := msg.Body().(type) {
	case *datasource.ContextUrlValues:
		for i, key := range cols {
			if val, ok := mt.Get(key); ok && !val.Nil() {
				dest[i] = val.Value()
				//u.Infof("key=%v   val=%v", key, val)
			} else {
				u.Warnf("missing value? %v %T %v", key, val.Value(), val.Value())
			}
		}
		//u.Debugf("got msg in row result writer: %#v", mt)
	case *datasource.ContextSimple:
		for i, key := range cols {
			//u.Debugf("mt = nil? %v", mt)
			if val, ok := mt.Get(key); ok && val != nil && !val.Nil() {
				dest[i] = val.Value()
				//u.Infof("key=%v   val=%v", key, val)
			} else if val == nil {
				u.Errorf("could not evaluate? %v", key)
			} else {
				u.Warnf("missing value? %v %T %v", key, val.Value(), val.Value())
			}
		}
		//u.Debugf("got msg in row result writer: %#v", mt)
	default:
		u.Errorf("unknown message type: %T", mt)
	}
	return nil
}
