package elasticsearch

import (
	"io"

	"database/sql/driver"
	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/expr"
)

var (
	_ ResultProvider = (*ResultReader)(nil)
)

type ResultProvider interface {
	driver.Rows
}

// Elasticsearch ResultReader
// - driver.Rows
// - ??  how do we get schema?
type ResultReader struct {
	*exec.TaskBase
	cols     []string
	Docs     []u.JsonHelper
	Total    int
	Aggs     u.JsonHelper
	ScrollId string
	Request  *SqlToEs
}

func NewResultReader() *ResultReader {
	m := &ResultReader{
		TaskBase: exec.NewTaskBase("ElasticsearchResultWriter"),
	}
	m.Handler = resultWrite(m)
	return m
}

func NewResultRows(req *SqlToEs) *ResultReader {
	stepper := exec.NewTaskStepper("elasticsearch.ResultRowWriter")
	m := &ResultReader{
		TaskBase: stepper.TaskBase,
	}
	m.Request = req
	return m
}

func NewMemResultReader(writeTo *[]datasource.Message) *ResultReader {
	m := &ResultReader{
		TaskBase: exec.NewTaskBase("elasticsearch.MemResultWriter"),
	}
	m.Handler = func(ctx *exec.Context, msg datasource.Message) bool {
		*writeTo = append(*writeTo, msg)
		u.Infof("write to msgs: %v", len(*writeTo))
		return true
	}
	return m
}

func (m *ResultReader) Copy() *ResultReader { return NewResultReader() }
func (m *ResultReader) Close() error        { return nil }
func (m *ResultReader) Next(dest []driver.Value) error {
	select {
	case msg, ok := <-m.MessageIn():
		if !ok {
			return io.EOF
		}
		if msg == nil {
			u.Warnf("nil message?")
			return io.EOF
			//return fmt.Errorf("Nil message error?")
		}
		return msgToRow(msg, m.cols, dest)
	case <-m.SigChan():
		return exec.ShuttingDownError
	}
}

func (m *ResultReader) Columns() []string {
	return m.cols
}

func resultWrite(m *ResultReader) exec.MessageHandler {
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
			u.Debugf("mt = nil? %v", mt)
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
