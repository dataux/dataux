package mysqlfe

import (
	"database/sql/driver"

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
	writer       models.ResultWriter
	schema       *schema.Schema
	proj         *rel.Projection
	Rs           *mysql.Resultset
	ctx          *plan.Context
	wroteHeaders bool
	*exec.TaskBase
}

type MySqlExecResultWriter struct {
	writer models.ResultWriter
	schema *schema.Schema
	Rs     *mysql.Result
	ctx    *plan.Context
	*exec.TaskBase
}

func NewMySqlResultWriter(writer models.ResultWriter, ctx *plan.Context) *MySqlResultWriter {

	m := &MySqlResultWriter{writer: writer, ctx: ctx, schema: ctx.Schema}
	if ctx.Projection != nil {
		m.proj = ctx.Projection.Proj
	} else {
		u.Warnf("no projection????")
	}

	m.TaskBase = exec.NewTaskBase(ctx)
	m.Rs = mysql.NewResultSet()

	m.Handler = resultWrite(m)
	return m
}

func NewMySqlSchemaWriter(writer models.ResultWriter, ctx *plan.Context) *MySqlResultWriter {

	m := &MySqlResultWriter{writer: writer, ctx: ctx, schema: ctx.Schema}
	m.proj = ctx.Projection.Proj
	m.TaskBase = exec.NewTaskBase(ctx)
	m.Rs = mysql.NewResultSet()

	m.Handler = schemaWrite(m)
	return m
}

func (m *MySqlResultWriter) Close() error {

	if m.Rs == nil || len(m.Rs.Fields) == 0 {
		m.Rs = NewEmptyResultset(m.ctx.Projection)
		//u.Infof("nil resultwriter Close() has RS?%v rowct:%v", m.Rs == nil, len(m.Rs.RowDatas))
	} else {
		//u.Infof("in mysql resultwriter Close() has RS?%v rowct:%v", m.Rs == nil, len(m.Rs.RowDatas))
	}
	m.writer.WriteResult(m.Rs)
	return nil
}
func schemaWrite(m *MySqlResultWriter) exec.MessageHandler {

	return func(_ *plan.Context, msg schema.Message) bool {

		//u.Debugf("in schemaWrite:  %#v", msg)
		if !m.wroteHeaders {
			m.WriteHeaders()
		}

		select {
		case <-m.SigChan():
			return false
		default:
			//ok
		}

		switch mt := msg.Body().(type) {
		case *schema.Field:
			// Got a single field, one field = row
			//u.Debugf("write field %#v", fieldDescribe(m.proj, mt))
			m.Rs.AddRowValues(fieldDescribe(m.proj, mt))
			return true
		case *datasource.SqlDriverMessageMap:
			//u.Infof("write: %#v", mt.Values())
			m.Rs.AddRowValues(mt.Values())
			//u.Debugf( "return from mysql.resultWrite")
			return true
		case map[string]driver.Value:
			vals := make([]driver.Value, len(m.proj.Columns))
			for _, col := range m.proj.Columns {
				if val, ok := mt[col.As]; !ok {
					u.Warnf("could not find result val: %v name=%s", col.As, col.Name)
				} else {
					//u.Debugf("found col: %#v    val=%#v", col, val)
					vals[col.ColPos] = val
				}
			}
			m.Rs.AddRowValues(vals)
			return true
		case []driver.Value:
			//u.Debugf("got msg in result writer: %#v", mt)
			m.Rs.AddRowValues(mt)
			return true
		}

		u.Errorf("could not convert to message reader: %T", msg.Body())
		return false
	}
}

func resultWrite(m *MySqlResultWriter) exec.MessageHandler {

	return func(_ *plan.Context, msg schema.Message) bool {

		//u.Debugf("in resultWrite:  %#v", msg)
		if !m.wroteHeaders {
			m.WriteHeaders()
		}

		select {
		case <-m.SigChan():
			return false
		default:
			//ok
		}

		switch mt := msg.Body().(type) {
		case *datasource.SqlDriverMessageMap:
			// u.Infof("write: %#v", mt.Values())
			// for _, v := range mt.Values() {
			// 	u.Debugf("v = %T = %v", v, v)
			// }
			m.Rs.AddRowValues(mt.Values())
			//u.Debugf( "return from mysql.resultWrite")
			return true
		case map[string]driver.Value:
			vals := make([]driver.Value, len(m.proj.Columns))
			for _, col := range m.proj.Columns {
				if val, ok := mt[col.As]; !ok {
					u.Warnf("could not find result val: %v name=%s", col.As, col.Name)
				} else {
					u.Debugf("found col: %#v    val=%#v", col, val)
					vals[col.ColPos] = val
				}
			}
			m.Rs.AddRowValues(vals)
			return true
		case []driver.Value:
			//u.Debugf("got msg in result writer: %#v", mt)
			m.Rs.AddRowValues(mt)
			return true
		}

		u.Errorf("could not convert to message reader: %T", msg.Body())
		return false
	}
}

func (m *MySqlResultWriter) WriteHeaders() error {

	s := m.schema
	if s == nil {
		panic("no schema")
	}
	//u.LogTracef(u.WARN, "wat?")
	if m.proj == nil {
		u.Warnf("no projection")
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
		//u.Debugf("writeheader %s %v", col.Name, col.Type.String())
		switch col.Type {
		case value.IntType:
			m.Rs.Fields = append(m.Rs.Fields, mysql.NewField(as, s.Name, s.Name, 32, mysql.MYSQL_TYPE_LONG))
		case value.StringType:
			m.Rs.Fields = append(m.Rs.Fields, mysql.NewField(as, s.Name, s.Name, 200, mysql.MYSQL_TYPE_STRING))
		case value.NumberType:
			m.Rs.Fields = append(m.Rs.Fields, mysql.NewField(as, s.Name, s.Name, 32, mysql.MYSQL_TYPE_FLOAT))
		case value.BoolType:
			m.Rs.Fields = append(m.Rs.Fields, mysql.NewField(as, s.Name, s.Name, 1, mysql.MYSQL_TYPE_TINY))
		case value.TimeType:
			m.Rs.Fields = append(m.Rs.Fields, mysql.NewField(as, s.Name, s.Name, 32, mysql.MYSQL_TYPE_DATETIME))
		case value.ByteSliceType:
			m.Rs.Fields = append(m.Rs.Fields, mysql.NewField(as, s.Name, s.Name, 32, mysql.MYSQL_TYPE_BLOB))
		default:
			u.Warnf("Field type not known explicitly mapped type=%v so use json %#v", col.Type.String(), col)
			m.Rs.Fields = append(m.Rs.Fields, mysql.NewField(as, s.Name, s.Name, 32, mysql.MYSQL_TYPE_BLOB))
		}
		//u.Debugf("added field: %v", col.Name)
	}

	//u.Debugf("writeheaders: %#v", m.Rs.FieldNames)
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
	//u.Debugf("projection: %#v", pp)
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
		//u.Debugf("field: %#v", col)
	}
	return r
}

func NewMySqlExecResultWriter(writer models.ResultWriter, ctx *plan.Context) *MySqlExecResultWriter {

	m := &MySqlExecResultWriter{writer: writer, ctx: ctx, schema: ctx.Schema}
	m.TaskBase = exec.NewTaskBase(m.Ctx)
	m.Rs = mysql.NewResult()
	m.Handler = nilWriter(m)
	return m
}
func (m *MySqlExecResultWriter) Close() error {
	if m.writer == nil {
		u.Warnf("wat?  nil writer? ")
	}
	//u.Debugf("rs?%#v  writer?%#v", m.Rs, m.writer)
	// TODO: we need to send a message to get this count
	m.Rs.AffectedRows = 1
	m.writer.WriteResult(m.Rs)
	return nil
}
func (m *MySqlExecResultWriter) Finalize() error {
	return nil
}
func nilWriter(m *MySqlExecResultWriter) exec.MessageHandler {
	return func(_ *plan.Context, msg schema.Message) bool {
		u.Debugf("in nilWriter:  %#v", msg)
		return false
	}
}
