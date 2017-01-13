package mysqlfe

import (
	"database/sql/driver"
	"errors"
	"time"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/plan"
	"github.com/araddon/qlbridge/rel"
	"github.com/araddon/qlbridge/schema"
	"github.com/araddon/qlbridge/value"

	"github.com/dataux/dataux/models"
	"github.com/dataux/dataux/vendored/mixer/mysql"
)

var (
	_ exec.TaskRunner = (*MySqlResultWriter)(nil)
	_ exec.TaskRunner = (*MySqlExecResultWriter)(nil)
)

type MySqlResultWriter struct {
	*exec.TaskBase
	closed       bool
	flushed      bool
	writer       models.ResultWriter
	msghandler   exec.MessageHandler
	schema       *schema.Schema
	proj         *rel.Projection
	Rs           *mysql.Resultset
	complete     chan bool
	isComplete   bool
	wroteHeaders bool
}

type MySqlExecResultWriter struct {
	*exec.TaskBase
	closed bool
	writer models.ResultWriter
	schema *schema.Schema
	Rs     *mysql.Result
	ct     int64
	err    error
}

func NewMySqlResultWriter(writer models.ResultWriter, ctx *plan.Context) *MySqlResultWriter {

	m := &MySqlResultWriter{writer: writer, schema: ctx.Schema, complete: make(chan bool)}
	if ctx.Projection != nil {
		if ctx.Projection.Proj != nil {
			m.proj = ctx.Projection.Proj
		}
	}

	m.TaskBase = exec.NewTaskBase(ctx)
	m.Rs = mysql.NewResultSet()
	m.msghandler = resultWrite(m)
	return m
}

func NewMySqlSchemaWriter(writer models.ResultWriter, ctx *plan.Context) *MySqlResultWriter {

	m := &MySqlResultWriter{writer: writer, schema: ctx.Schema, complete: make(chan bool)}
	m.proj = ctx.Projection.Proj

	m.TaskBase = exec.NewTaskBase(ctx)
	m.Rs = mysql.NewResultSet()

	m.msghandler = resultWrite(m)
	return m
}

func (m *MySqlResultWriter) Close() error {
	if m.closed {
		return nil
	}
	m.closed = true

	if err := m.flushResults(); err != nil {
		u.Errorf("could not flush? %v", err)
	}

	return m.TaskBase.Close()
}

func (m *MySqlResultWriter) flushResults() error {
	//u.Infof("%p mysql flushResults() already flushed?%v", m, m.flushed)
	if m.flushed {
		return nil
	}
	m.flushed = true

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	//u.Infof("%p mysql Close() waiting for complete", m)
	select {
	case <-ticker.C:
		u.Warnf("mysql write timeout???? ")
	case <-m.complete:
		//u.Debugf("%p got mysql result complete", m)
	}
	if !m.wroteHeaders {
		m.WriteHeaders()
	}
	if m.Rs == nil || len(m.Rs.Fields) == 0 {
		m.Rs = NewEmptyResultset(m.Ctx.Projection)
	}
	m.writer.WriteResult(m.Rs)
	return nil
}

func (m *MySqlResultWriter) Run() error {
	defer m.Ctx.Recover()
	inCh := m.MessageIn()

	for {

		select {
		case <-m.SigChan():
			u.Debugf("got signal quit")
			return nil
		case msg, ok := <-inCh:
			if !ok || msg == nil {
				//u.Debugf("%p MYSQL INPUT CLOSED, got msg shutdown nilmsg?%v", m, msg == nil)
				if !m.isComplete {
					m.isComplete = true
					close(m.complete)
					return m.flushResults()
				}
				return nil
			}

			if ok := m.msghandler(nil, msg); !ok {
				u.Warnf("wat, not ok? %v", msg)
			}
		}
	}
	return nil
}
func resultWrite(m *MySqlResultWriter) exec.MessageHandler {

	return func(_ *plan.Context, msg schema.Message) bool {

		if msg == nil {
			return false
		}

		if !m.wroteHeaders {
			m.WriteHeaders()
		}

		// Watch for shutdown
		select {
		case <-m.SigChan():
			return false
		default:
		}

		switch mt := msg.Body().(type) {
		case *schema.Field:
			// Got a single field, one field = row
			m.Rs.AddRowValues(fieldDescribe(m.proj, mt))
			return true

		case *datasource.SqlDriverMessageMap:

			// If we don't need to zero-fill missing columns
			if len(mt.Vals) == len(m.proj.Columns) {
				m.Rs.AddRowValues(mt.Values())
				return true
			}

			// We need to create a sparse array
			vals := make([]driver.Value, len(m.proj.Columns))
			for _, col := range m.proj.Columns {
				idx := mt.ColIndex[col.As]
				if len(mt.Vals) > idx {
					vals[col.ColPos] = mt.Vals[idx]
				}
			}
			m.Rs.AddRowValues(vals)
			return true

		case map[string]driver.Value:
			vals := make([]driver.Value, len(m.proj.Columns))
			for _, col := range m.proj.Columns {
				if val, ok := mt[col.As]; ok {
					vals[col.ColPos] = val
				}
			}
			m.Rs.AddRowValues(vals)
			return true
		case []driver.Value:
			m.Rs.AddRowValues(mt)
			return true
		}

		return false
	}
}

func (m *MySqlResultWriter) WriteHeaders() error {

	s := m.schema
	if s == nil {
		panic("no schema")
	}

	cols := m.proj.Columns
	//u.Debugf("projection: %p writing mysql headers %s", m.projection.Proj, m.projection.Proj)
	if len(cols) == 0 {
		u.Warnf("Wat?   no columns?   %v", 0)
		return nil
	}

	m.wroteHeaders = true
	wasWriten := make(map[string]struct{}, len(cols))
	for i, col := range cols {
		as := col.Name
		if col.Col != nil {
			as = col.Col.As
		}
		if _, exists := wasWriten[col.Name]; exists {
			continue
		}
		wasWriten[col.Name] = struct{}{}
		m.Rs.FieldNames[col.Name] = i

		switch col.Type {
		case value.IntType:
			m.Rs.Fields = append(m.Rs.Fields, mysql.NewField(as, s.Name, s.Name, 8, mysql.MYSQL_TYPE_LONG))
		case value.StringType:
			m.Rs.Fields = append(m.Rs.Fields, mysql.NewField(as, s.Name, s.Name, 200, mysql.MYSQL_TYPE_STRING))
		case value.NumberType:
			m.Rs.Fields = append(m.Rs.Fields, mysql.NewField(as, s.Name, s.Name, 8, mysql.MYSQL_TYPE_FLOAT))
		case value.BoolType:
			m.Rs.Fields = append(m.Rs.Fields, mysql.NewField(as, s.Name, s.Name, 1, mysql.MYSQL_TYPE_TINY))
		case value.TimeType:
			m.Rs.Fields = append(m.Rs.Fields, mysql.NewField(as, s.Name, s.Name, 8, mysql.MYSQL_TYPE_DATETIME))
		case value.ByteSliceType:
			m.Rs.Fields = append(m.Rs.Fields, mysql.NewField(as, s.Name, s.Name, 32, mysql.MYSQL_TYPE_BLOB))
		case value.JsonType:
			m.Rs.Fields = append(m.Rs.Fields, mysql.NewField(as, s.Name, s.Name, 256, mysql.MYSQL_TYPE_JSON))
		default:
			u.Debugf("Field type not known explicitly mapped type=%v so use json %#v", col.Type.String(), col)
			m.Rs.Fields = append(m.Rs.Fields, mysql.NewField(as, s.Name, s.Name, 32, mysql.MYSQL_TYPE_BLOB))
		}
	}
	return nil
}

func (m *MySqlResultWriter) Finalize() error {
	if !m.wroteHeaders {
		err := m.WriteHeaders()
		if err != nil {
			return err
		}
	}
	return nil
}

func NewEmptyResultset(pp *plan.Projection) *mysql.Resultset {

	r := new(mysql.Resultset)
	r.Fields = make([]*mysql.Field, len(pp.Proj.Columns))
	r.FieldNames = make(map[string]int, len(r.Fields))
	r.Values = make([][]driver.Value, 0)
	r.RowDatas = make([]mysql.RowData, 0)

	for i, col := range pp.Proj.Columns {
		r.Fields[i] = &mysql.Field{}
		switch {
		case col.Star:
			r.Fields[i].Name = []byte("*")
		// case col.Tree != nil:
		// 	if col.As != col.Key() {
		// 		r.Fields[i].Name = []byte(col.As)
		// 		r.Fields[i].OrgName = hack.Slice(col.String())
		// 	} else {
		// 		r.Fields[i].Name = hack.Slice(col.String())
		// 	}
		default:
			r.Fields[i].Name = []byte(col.As)
		}
	}
	return r
}

func NewMySqlExecResultWriter(writer models.ResultWriter, ctx *plan.Context) *MySqlExecResultWriter {

	m := &MySqlExecResultWriter{writer: writer, schema: ctx.Schema}
	m.TaskBase = exec.NewTaskBase(ctx)
	m.Rs = mysql.NewResult()
	m.Handler = m.ResultWriter()
	return m
}
func (m *MySqlExecResultWriter) Close() error {
	if m.closed {
		return nil
	}
	m.closed = true
	if m.writer == nil {
		u.Warnf("wat?  nil writer? ")
	}
	if m.err != nil {
		m.Rs.Status = mysql.ER_UNKNOWN_ERROR
	} else {
		m.Rs.AffectedRows = uint64(m.ct)
	}
	m.writer.WriteResult(m.Rs)
	return m.TaskBase.Close()
}
func (m *MySqlExecResultWriter) Finalize() error {
	return nil
}
func (m *MySqlExecResultWriter) ResultWriter() exec.MessageHandler {
	return func(_ *plan.Context, msg schema.Message) bool {

		var vals []driver.Value
		switch mt := msg.Body().(type) {
		case *datasource.SqlDriverMessageMap:
			vals = mt.Values()
		case []driver.Value:
			vals = mt
		default:
			u.Warnf("%T not supported", mt)
			return false
		}
		if len(vals) == 2 {
			switch rt := vals[0].(type) {
			case string: // error
				m.err = errors.New(rt)
			default:
				if affectedCt, isInt := vals[1].(int64); isInt {
					m.ct = affectedCt
				}
			}
			return true
		}
		return false
	}
}
