package elasticsearch

import (
	"database/sql/driver"
	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
	"github.com/dataux/dataux/pkg/models"
	"github.com/dataux/dataux/vendor/mixer/mysql"
	"github.com/dataux/dataux/vendor/mixer/proxy"
	"io"
)

var (
// What is the interface we want to implement here?
//_ ResultProvider = (*MysqlResultWriter)(nil)
)

type MysqlResultWriter struct {
	resp         ResultProvider
	rs           *mysql.Resultset
	sql          *expr.SqlSelect
	tbl          *models.Table
	conn         *proxy.Conn
	wroteHeaders bool
}

func NewMysqlResultWriter(conn *proxy.Conn, sql *expr.SqlSelect, resp ResultProvider, tbl *models.Table) *MysqlResultWriter {
	m := &MysqlResultWriter{sql: sql, conn: conn, resp: resp}
	m.tbl = tbl
	m.rs = mysql.NewResultSet()
	return m
}

func (m *MysqlResultWriter) WriteHeaders() error {

	m.wroteHeaders = true
	s := m.tbl.Schema
	tbl := m.tbl
	cols := m.resp.Columns()
	if s == nil {
		panic("no schema")
	}
	if m.sql.Star {
		m.rs.Fields = tbl.FieldsMySql
		m.rs.FieldNames = tbl.FieldPositions
	} else {
		for i, col := range cols {
			as := col.Name
			if col.SqlCol != nil {
				as = col.SqlCol.As
			}
			m.rs.FieldNames[col.Name] = i
			switch col.Type {
			case value.IntType:
				m.rs.Fields = append(m.rs.Fields, mysql.NewField(as, s.Db, s.Db, 32, mysql.MYSQL_TYPE_LONG))
			case value.StringType:
				m.rs.Fields = append(m.rs.Fields, mysql.NewField(as, s.Db, s.Db, 200, mysql.MYSQL_TYPE_STRING))
			case value.NumberType:
				m.rs.Fields = append(m.rs.Fields, mysql.NewField(as, s.Db, s.Db, 32, mysql.MYSQL_TYPE_FLOAT))
			default:
				u.Warnf("Field type not known: %#v", col)
			}
		}

	}
	u.Debugf("writeheaders: %#v", m.rs.FieldNames)
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
		m.rs.AddRowValues(vals)

	}

	return nil
}
