package backends

import (
	"database/sql/driver"
	"io"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
	"github.com/dataux/dataux/pkg/models"
	"github.com/dataux/dataux/vendor/mixer/mysql"
)

var (
// What is the interface we want to implement here?
//_ ResultProvider = (*MysqlResultWriter)(nil)
)

type MysqlResultWriter struct {
	resp         models.ResultProvider
	Rs           *mysql.Resultset
	sql          *expr.SqlSelect
	wroteHeaders bool
}

func NewMysqlResultWriter(sql *expr.SqlSelect, resp models.ResultProvider) *MysqlResultWriter {
	m := &MysqlResultWriter{sql: sql, resp: resp}
	m.Rs = mysql.NewResultSet()
	return m
}

func (m *MysqlResultWriter) WriteHeaders() error {

	m.wroteHeaders = true
	s := m.resp.Schema()
	cols := m.resp.Columns()
	if s == nil {
		panic("no schema")
	}
	for i, col := range cols {
		as := col.Name
		if col.SqlCol != nil {
			as = col.SqlCol.As
		}
		m.Rs.FieldNames[col.Name] = i
		switch col.Type {
		case value.IntType:
			m.Rs.Fields = append(m.Rs.Fields, mysql.NewField(as, s.Db, s.Db, 32, mysql.MYSQL_TYPE_LONG))
		case value.StringType:
			m.Rs.Fields = append(m.Rs.Fields, mysql.NewField(as, s.Db, s.Db, 200, mysql.MYSQL_TYPE_STRING))
		case value.NumberType:
			m.Rs.Fields = append(m.Rs.Fields, mysql.NewField(as, s.Db, s.Db, 32, mysql.MYSQL_TYPE_FLOAT))
		default:
			u.Warnf("Field type not known: %#v", col)
		}
	}

	u.Debugf("writeheaders: %#v", m.Rs.FieldNames)
	return nil
}

func (m *MysqlResultWriter) Finalize() error {
	if !m.wroteHeaders {
		err := m.WriteHeaders()
		if err != nil {
			return err
		}
	}

	vals := make([]driver.Value, len(m.resp.Columns()))
	for {

		err := m.resp.Next(vals)
		if err != nil && err == io.EOF {
			break
		} else if err != nil {
			u.Error(err)
			return err
		}
		u.Debugf("vals: %v", vals)
		m.Rs.AddRowValues(vals)
	}

	return nil
}
