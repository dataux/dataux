package elasticsearch

import (
	"encoding/json"
	"fmt"

	//"database/sql/driver"
	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/datasource"
	//"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
	"github.com/dataux/dataux/vendor/mixer/mysql"
	"github.com/dataux/dataux/vendor/mixer/proxy"
)

/*
TODO:
- Refactor MysqlResultWriter to




*/
var (
	_ ResultProvider = (*ResultReader)(nil)

	// Ensure we implement datasource.DataSource
	_ datasource.DataSource = (*ResultReader)(nil)
)

// ResultProvider is a Result Interface for reading rows
//   also provides schema
type ResultProvider interface {
	//driver.Rows
}

type MysqlResultWriter struct {
	resp *ResultReader
	rs   *mysql.Resultset
	sql  *expr.SqlSelect
	req  *SqlToEs
	conn *proxy.Conn
}

func NewMysqlResultWriter(conn *proxy.Conn, sql *expr.SqlSelect, resp *ResultReader, sqlEs *SqlToEs) *MysqlResultWriter {
	m := &MysqlResultWriter{sql: sql, conn: conn, resp: resp, req: sqlEs}
	m.rs = mysql.NewResultSet()
	return m
}

func (m *MysqlResultWriter) WriteHeaders() error {

	s := m.req.schema
	tbl := m.req.tbl
	if s == nil {
		panic("no schema")
	}
	if m.sql.Star {
		m.rs.Fields = tbl.FieldsMySql
		m.rs.FieldNames = tbl.FieldPositions
	} else if m.sql.CountStar() {
		// Count(*)
		m.rs.FieldNames["count"] = 0
		m.rs.Fields = append(m.rs.Fields, mysql.NewField("count", s.Db, s.Db, 32, mysql.MYSQL_TYPE_LONG))
	} else if len(m.resp.Aggs) > 0 {
		for _, col := range m.sql.Columns {
			switch fnexpr := col.Tree.Root.(type) {
			case *expr.FuncNode:
				switch fnexpr.Name {
				case "terms":
					//
				case "min", "max", "avg", "sum":
					m.rs.Fields = append(m.rs.Fields, mysql.NewField(col.As, s.Db, s.Db, 32, mysql.MYSQL_TYPE_FLOAT))
				case "count", "cardinality":
					m.rs.Fields = append(m.rs.Fields, mysql.NewField(col.As, s.Db, s.Db, 32, mysql.MYSQL_TYPE_LONG))
				}
			}
		}
		if m.req.hasMultiValue {
			// MultiValue returns are resultsets that have multiple rows for a single expression, ie top 10 terms for this field, etc
			m.rs.Fields = append(m.rs.Fields, mysql.NewField("field", s.Db, s.Db, 32, mysql.MYSQL_TYPE_STRING))
			m.rs.Fields = append(m.rs.Fields, mysql.NewField("key", s.Db, s.Db, 500, mysql.MYSQL_TYPE_STRING))
			m.rs.Fields = append(m.rs.Fields, mysql.NewField("count", s.Db, s.Db, 500, mysql.MYSQL_TYPE_STRING))
		}
	} else {
		namePos := 0
		for _, col := range m.sql.Columns {
			fldName := col.As
			if fld, ok := tbl.FieldMapMySql[col.SourceField]; ok {
				u.Debugf("looking for col: %v AS %v  %v", col.SourceField, fldName, mysql.TypeString(fld.Type))
				fldCopy := fld.Clone()
				fldCopy.NameOverride(col.As)
				//fld.FieldName = col.SourceField
				m.rs.Fields = append(m.rs.Fields, fldCopy)
				m.rs.FieldNames[fldName] = namePos
				namePos++
			} else {
				u.Warnf("not found? '%#v' ", col)
			}
		}
	}
	u.Debugf("writeheaders: %#v", m.rs.FieldNames)
	return nil
}

func (m *MysqlResultWriter) Finalize() error {
	iter := m.resp.CreateIterator(nil)
	for {
		msg := iter.Next()
		if msg == nil {
			break
		}
		if vals, ok := msg.Body().([]interface{}); ok {
			u.Debugf("found vals: len(fields)=%v len(vals)=%v %#v    %#v", len(m.rs.Fields), len(vals), vals, msg)
			m.rs.AddRowValues(vals)
		} else {
			return fmt.Errorf("Could not conver to []interface{}")
		}
	}

	return nil
}

/*

Source ->  Where  -> GroupBy/Counts etc  -> Projection -> ResultWriter

- Since we don't really need the Where, GroupBy, etc

Source ->    Projection  -> ResultWriter




*/

type ValsMessage struct {
	vals []interface{}
	id   uint64
}

func (m ValsMessage) Key() uint64       { return m.id }
func (m ValsMessage) Body() interface{} { return m.vals }

type ResultColumn struct {
	Name string          // Original path/name for query field
	Pos  int             // Ordinal position in sql statement
	Type value.ValueType // Type
}

// Elasticsearch ResultReader
// - driver.Rows
// - ??  how do we get schema?
type ResultReader struct {
	exit     <-chan bool
	cursor   int
	colnames []string
	Cols     []*ResultColumn
	Docs     []u.JsonHelper
	Vals     [][]interface{}
	Total    int
	Aggs     u.JsonHelper
	ScrollId string
	Req      *SqlToEs
}

func NewResultReader(req *SqlToEs) *ResultReader {
	m := &ResultReader{}
	m.Req = req
	return m
}

func (m *ResultReader) Close() error      { return nil }
func (m *ResultReader) Columns() []string { return m.colnames }
func (m *ResultReader) buildColumns() {

	m.Cols = make([]*ResultColumn, 0)
	sql := m.Req.sel
	if sql.Star {
		// Select Each field, grab fields from Table Schema
		for _, fld := range m.Req.tbl.Fields {
			m.Cols = append(m.Cols, &ResultColumn{fld.Name, len(m.Cols), fld.Type})
		}
	} else if sql.CountStar() {
		// Count *
		m.Cols = append(m.Cols, &ResultColumn{"count", len(m.Cols), value.IntType})
	} else if len(m.Aggs) > 0 {
		if m.Req.hasSingleValue {
			for _, col := range sql.Columns {
				if col.CountStar() {
					m.Cols = append(m.Cols, &ResultColumn{col.Key(), len(m.Cols), value.IntType})
				} else {
					u.Debugf("why Aggs? %#v", col)
					m.Cols = append(m.Cols, &ResultColumn{col.Key(), len(m.Cols), value.IntType})
				}
			}
		} else if m.Req.hasMultiValue {
			// MultiValue returns are resultsets that have multiple rows for a single expression, ie top 10 terms for this field, etc
			// if len(sql.GroupBy) > 0 {
			// We store the Field Name Here
			u.Debugf("why MultiValue Aggs? %#v", m.Req)
			m.Cols = append(m.Cols, &ResultColumn{"field_name", len(m.Cols), value.StringType})
			m.Cols = append(m.Cols, &ResultColumn{"key", len(m.Cols), value.StringType}) // the value of the field
			m.Cols = append(m.Cols, &ResultColumn{"count", len(m.Cols), value.IntType})
		}
	} else {
		for _, col := range m.Req.sel.Columns {
			if fld, ok := m.Req.tbl.FieldMap[col.SourceField]; ok {
				u.Debugf("column: %#v", col)
				m.Cols = append(m.Cols, &ResultColumn{col.SourceField, len(m.Cols), fld.Type})
			} else {
				u.Debugf("Could not find: %v", col.String())
			}
		}
	}
}

func (m *ResultReader) Open(connInfo string) (datasource.DataSource, error) {
	panic("Not implemented")
	return m, nil
}

func (m *ResultReader) CreateIterator(filter expr.Node) datasource.Iterator {
	return m
}

// Finalize maps the Es Documents/results into
//    [][]interface{}
//
//  Normally, finalize is responsible for ensuring schema, setu
//   but in the case of elasticsearch, since it is a non-streaming
//   response, we build out values in advance
func (m *ResultReader) Finalize() error {

	m.buildColumns()

	defer func() {
		u.Debugf("nice, finalize vals in ResultReader: %v", len(m.Vals))
	}()

	sql := m.Req.sel

	m.Vals = make([][]interface{}, 0)

	if sql.Star {
		// ??
	} else if sql.CountStar() {
		// Count *
		vals := make([]interface{}, 1)
		vals[0] = m.Total
		m.Vals = append(m.Vals, vals)
		return nil
	} else if len(m.Aggs) > 0 {

		if m.Req.hasMultiValue && m.Req.hasSingleValue {
			return fmt.Errorf("Must not mix single value and multi-value aggs")
		}
		if m.Req.hasSingleValue {
			vals := make([]interface{}, len(sql.Columns))
			for i, col := range sql.Columns {
				fldName := col.Key()
				if col.Tree != nil && col.Tree.Root != nil {
					u.Debugf("col: %v", col.Tree.Root.StringAST())
				}

				if col.CountStar() {
					u.Debugf("found count star")
					vals[i] = m.Total
				} else {
					u.Debugf("looking for col: %v %v %v", fldName, m.Aggs.Get(fldName+"/value"))
					vals[i] = m.Aggs.Get(fldName + "/value")
				}

			}
			u.Debugf("write result: %v", vals)
			m.Vals = append(m.Vals, vals)
		} else if m.Req.hasMultiValue {
			// MultiValue returns are resultsets that have multiple rows for a single expression, ie top 10 terms for this field, etc

			if len(sql.GroupBy) > 0 {
				//for i, col := range sql.Columns {
				for i, _ := range sql.GroupBy {
					fldName := fmt.Sprintf("group_by_%d", i)
					u.Debugf("looking for col: %v  %v", fldName, m.Aggs.Get(fldName+"/results"))
					results := m.Aggs.Helpers(fldName + "/buckets")
					for _, result := range results {
						vals := make([]interface{}, 3)
						vals[0] = fldName
						vals[1] = result.String("key")
						vals[2] = result.Int("doc_count")
						m.Vals = append(m.Vals, vals)
					}
					u.Debugf("missing value? %v", m.Aggs.Get(fldName))
					// by, _ := json.MarshalIndent(m.Aggs.Get(fldName), " ", " ")
					// vals[1] = by

				}
			} else {
				// MultiValue are generally aggregates
				for _, col := range sql.Columns {
					fldName := col.As
					u.Debugf("looking for col: %v  %v", fldName, m.Aggs.Get(fldName+"/results"))
					results := m.Aggs.Helpers(fldName + "/buckets")
					for _, result := range results {
						vals := make([]interface{}, 3)
						vals[0] = fldName
						vals[1] = result.String("key")
						vals[2] = result.Int("doc_count")
						m.Vals = append(m.Vals, vals)
					}
				}
			}
		}

		//return m.conn.WriteResultset(m.conn.Status, rs)
		return nil
	}

	metaFields := map[string]byte{"_id": 1, "_type": 1, "_score": 1}

	for _, doc := range m.Docs {
		if len(doc) > 0 {
			//by, _ := json.MarshalIndent(doc, " ", " ")
			//u.Debugf("doc: %v", string(by))
			vals := make([]interface{}, len(m.Cols))
			//for fldI, fld := range rs.Fields {
			fldI := 0
			if len(m.Cols) == 0 {
				u.Errorf("WTF?  no cols? %v", m.Cols)
			}
			for _, col := range m.Cols {
				// key := "_source." + fld.FieldName
				// if _, ok := metaFields[fld.FieldName]; ok {
				// 	key = fld.FieldName
				// }
				// //u.Debugf("field: %s type=%v key='%s' %v", fld.Name, mysql.TypeString(fld.Type), key, doc.String(key))
				// switch fld.Type {
				// case mysql.MYSQL_TYPE_STRING:
				// 	vals[fldI] = doc.String(key)
				// case mysql.MYSQL_TYPE_DATETIME:
				// 	vals[fldI] = doc.String(key)
				// case mysql.MYSQL_TYPE_LONG:
				// 	vals[fldI] = doc.Int64(key)
				// case mysql.MYSQL_TYPE_FLOAT:
				// 	vals[fldI] = doc.Float64(key)
				// case mysql.MYSQL_TYPE_BLOB:
				// 	u.Debugf("blob?  %v", key)
				// 	if docVal := doc.Get(key); docVal != nil {
				// 		by, _ := json.Marshal(docVal)
				// 		vals[fldI] = string(by)
				// 	}
				// default:
				// 	u.Warnf("unrecognized type: %v", fld.String())
				// }
				key := "_source." + col.Name
				if _, ok := metaFields[col.Name]; ok {
					key = col.Name
					u.Debugf("looking for? %v in %#v", key, doc)
				}
				u.Debugf("field: %s type=%v key='%s' %v", col.Name, col.Type.String(), key, doc.String(key))
				switch col.Type {
				case value.StringType:
					vals[fldI] = doc.String(key)
				case value.TimeType:
					vals[fldI] = doc.String(key)
				case value.IntType:
					vals[fldI] = doc.Int64(key)
				case value.NumberType:
					vals[fldI] = doc.Float64(key)
				case value.ByteSliceType:
					u.Debugf("blob?  %v", key)
					if docVal := doc.Get(key); docVal != nil {
						by, _ := json.Marshal(docVal)
						vals[fldI] = string(by)
					}
				default:
					u.Warnf("unrecognized type: %v  %T", col.Name, col.Type)
				}
				fldI++
			}
			m.Vals = append(m.Vals, vals)
		}
	}

	return nil
}

func (m *ResultReader) Next() datasource.Message {
	select {
	case <-m.exit:
		return nil
	default:
		for {

			if m.cursor >= len(m.Vals) {
				return nil
			}
			m.cursor++
			u.Debugf("ResultReader.Next():  cursor:%v  %v", m.cursor, len(m.Vals[m.cursor-1]))
			return ValsMessage{m.Vals[m.cursor-1], uint64(m.cursor)}
		}

	}
}
