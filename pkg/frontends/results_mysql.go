package frontends

import (
	"database/sql/driver"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"

	"github.com/dataux/dataux/pkg/models"
	"github.com/dataux/dataux/vendor/mixer/mysql"
)

var (
	_ exec.TaskRunner = (*MySqlResultWriter)(nil)
	_ exec.TaskRunner = (*MySqlExecResultWriter)(nil)
)

type MySqlResultWriter struct {
	writer       models.ResultWriter
	schema       *datasource.Schema
	Rs           *mysql.Resultset
	projection   *expr.Projection
	wroteHeaders bool
	*exec.TaskBase
}

type MySqlExecResultWriter struct {
	writer models.ResultWriter
	schema *datasource.Schema
	Rs     *mysql.Result
	*exec.TaskBase
}

func NewMySqlResultWriter(writer models.ResultWriter, proj *expr.Projection, schema *datasource.Schema) *MySqlResultWriter {

	m := &MySqlResultWriter{writer: writer, projection: proj, schema: schema}
	m.TaskBase = exec.NewTaskBase("MySqlResultWriter")
	m.Rs = mysql.NewResultSet()
	m.Handler = resultWrite(m)
	return m
}
func (m *MySqlResultWriter) Close() error {

	if m.Rs == nil || len(m.Rs.Fields) == 0 {
		m.Rs = NewEmptyResultset(m.projection)
		//u.Infof("nil resultwriter Close() has RS?%v rowct:%v", m.Rs == nil, len(m.Rs.RowDatas))
	} else {
		//u.Infof("in mysql resultwriter Close() has RS?%v rowct:%v", m.Rs == nil, len(m.Rs.RowDatas))
	}
	m.writer.WriteResult(m.Rs)
	return nil
}

func resultWrite(m *MySqlResultWriter) exec.MessageHandler {

	return func(_ *exec.Context, msg datasource.Message) bool {

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
			m.Rs.AddRowValues(mt.Values())
			return true
		case map[string]driver.Value:
			vals := make([]driver.Value, len(m.projection.Columns))
			for _, col := range m.projection.Columns {
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
			//u.Debugf("got msg in result writer: %#v", vals)
			m.Rs.AddRowValues(mt)
			return true
		}

		u.Errorf("could not convert to message reader: %T", msg.Body())
		return false
	}
}

func (m *MySqlResultWriter) WriteHeaders() error {

	//u.LogTracef(u.WARN, "wat?")
	if m.projection == nil {
		u.Warnf("no projection")
	}
	cols := m.projection.Columns
	//u.Debugf("writing mysql headers: %v", cols)
	if len(cols) == 0 {
		u.Warnf("Wat?   no columns?   %v", 0)
		return nil
	}
	m.wroteHeaders = true
	s := m.schema
	if s == nil {
		panic("no schema")
	}
	for i, col := range cols {
		as := col.Name
		if col.Col != nil {
			as = col.Col.As
		}
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
			u.Warnf("Field type not known explicitly mapped type=%v  %#v", col.Type.String(), col)
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

	// vals := make([]driver.Value, len(m.resp.Columns()))
	// for {

	// 	err := m.resp.Next(vals)
	// 	if err != nil && err == io.EOF {
	// 		break
	// 	} else if err != nil {
	// 		u.Error(err)
	// 		return err
	// 	}
	// 	u.Debugf("vals: %v", vals)
	// 	m.Rs.AddRowValues(vals)
	// }

	return nil
}

func NewEmptyResultset(proj *expr.Projection) *mysql.Resultset {

	r := new(mysql.Resultset)
	//u.Debugf("projection: %#v", proj)
	r.Fields = make([]*mysql.Field, len(proj.Columns))

	for i, col := range proj.Columns {
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

	r.Values = make([][]driver.Value, 0)
	r.RowDatas = make([]mysql.RowData, 0)

	return r
}

func NewMySqlExecResultWriter(writer models.ResultWriter, schema *datasource.Schema) *MySqlExecResultWriter {

	m := &MySqlExecResultWriter{writer: writer, schema: schema}
	m.TaskBase = exec.NewTaskBase("MySqlExecResultWriter")
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
	return func(_ *exec.Context, msg datasource.Message) bool {
		u.Debugf("in nilWriter:  %#v", msg)
		return false
	}
}
