package mongo

import (
	"database/sql/driver"
	"io"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
	"github.com/dataux/dataux/pkg/models"
)

var (
	_ models.ResultProvider = (*ResultReader)(nil)

	// Ensure we implement datasource.DataSource, Scanner
	_ datasource.DataSource = (*ResultReader)(nil)
	_ datasource.Scanner    = (*ResultReader)(nil)
)

// Mongo ResultProvider
// - driver.Rows
type ResultReader struct {
	exit          <-chan bool
	finalized     bool
	hasprojection bool
	cursor        int
	proj          *expr.Projection
	Docs          []u.JsonHelper
	Vals          [][]driver.Value
	Total         int
	Aggs          u.JsonHelper
	ScrollId      string
	query         *mgo.Query
	Req           *SqlToMgo
}

// A wrapper, allowing us to implement sql/driver Next() interface
//   which is different than qlbridge/datasource Next()
type ResultReaderNext struct {
	*ResultReader
}

func NewResultReader(req *SqlToMgo, q *mgo.Query) *ResultReader {
	m := &ResultReader{}
	m.query = q
	m.Req = req
	return m
}

func (m *ResultReader) Close() error { return nil }

func (m *ResultReader) buildProjection() {

	if m.hasprojection {
		return
	}
	m.hasprojection = true
	m.proj = expr.NewProjection()
	cols := m.proj.Columns
	sql := m.Req.sel
	if sql.Star {
		// Select Each field, grab fields from Table Schema
		for _, fld := range m.Req.tbl.Fields {
			cols = append(cols, expr.NewResultColumn(fld.Name, len(cols), nil, fld.Type))
		}
	} else if sql.CountStar() {
		// Count *
		cols = append(cols, expr.NewResultColumn("count", len(cols), nil, value.IntType))
	} else if len(m.Aggs) > 0 {
		if m.Req.hasSingleValue {
			for _, col := range sql.Columns {
				if col.CountStar() {
					cols = append(cols, expr.NewResultColumn(col.Key(), len(cols), col, value.IntType))
				} else {
					u.Debugf("why Aggs? %#v", col)
					cols = append(cols, expr.NewResultColumn(col.Key(), len(cols), col, value.IntType))
				}
			}
		} else if m.Req.hasMultiValue {
			// MultiValue returns are resultsets that have multiple rows for a single expression, ie top 10 terms for this field, etc
			// if len(sql.GroupBy) > 0 {
			// We store the Field Name Here
			u.Debugf("why MultiValue Aggs? %#v", m.Req)
			cols = append(cols, expr.NewResultColumn("field_name", len(cols), nil, value.StringType))
			cols = append(cols, expr.NewResultColumn("key", len(cols), nil, value.StringType)) // the value of the field
			cols = append(cols, expr.NewResultColumn("count", len(cols), nil, value.IntType))
		}
	} else {
		for _, col := range m.Req.sel.Columns {
			if fld, ok := m.Req.tbl.FieldMap[col.SourceField]; ok {
				//u.Debugf("column: %#v", col)
				cols = append(cols, expr.NewResultColumn(col.SourceField, len(cols), col, fld.Type))
			} else {
				u.Debugf("Could not find: %v", col.String())
			}
		}
	}
	m.proj.Columns = cols
	//u.Debugf("leaving Columns:  %v", len(m.proj.Columns))
}

func (m *ResultReader) Tables() []string {
	return nil
}

func (m *ResultReader) Projection() (*expr.Projection, error) {
	m.buildProjection()
	return m.proj, nil
}

func (m *ResultReader) Open(connInfo string) (datasource.SourceConn, error) {
	panic("Not implemented")
	return m, nil
}

func (m *ResultReader) Schema() *models.Schema {
	return m.Req.tbl.Schema
}

func (m *ResultReader) MesgChan(filter expr.Node) <-chan datasource.Message {
	iter := m.CreateIterator(filter)
	return datasource.SourceIterChannel(iter, filter, m.exit)
}

func (m *ResultReader) CreateIterator(filter expr.Node) datasource.Iterator {
	return &ResultReaderNext{m}
}

// Finalize maps the Mongo Documents/results into
//    [][]interface{}   which is compabitble with sql/driver values
//
func (m *ResultReader) Finalize() error {

	m.finalized = true
	m.buildProjection()

	defer func() {
		u.Debugf("nice, finalize vals in ResultReader: %v", len(m.Vals))
	}()

	sql := m.Req.sel

	m.Vals = make([][]driver.Value, 0)

	if sql.CountStar() {
		// Count *
		vals := make([]driver.Value, 1)
		ct, err := m.query.Count()
		if err != nil {
			u.Errorf("could not get count: %v", err)
			return err
		}
		vals[0] = ct
		m.Vals = append(m.Vals, vals)
		return nil
	}

	cols := m.proj.Columns
	if len(cols) == 0 {
		u.Errorf("WTF?  no cols? %v", cols)
	}

	iter := m.query.Iter()
	for {
		var bm bson.M
		if !iter.Next(&bm) {
			break
		}
		vals := make([]driver.Value, len(cols))
		for i, col := range cols {
			if val, ok := bm[col.Name]; ok {
				vals[i] = val
			}
		}
		//u.Debugf("vals=%#v", vals)
		m.Vals = append(m.Vals, vals)
	}
	if err := iter.Close(); err != nil {
		u.Errorf("could not iter: %v", err)
		return err
	}

	return nil
}

// Implement sql/driver Rows Next() interface
func (m *ResultReader) Next(row []driver.Value) error {
	if m.cursor >= len(m.Vals) {
		return io.EOF
	}
	m.cursor++
	//u.Debugf("ResultReader.Next():  cursor:%v  %v", m.cursor, len(m.Vals[m.cursor-1]))
	for i, val := range m.Vals[m.cursor-1] {
		row[i] = val
	}
	return nil
}

func (m *ResultReaderNext) Next() datasource.Message {
	select {
	case <-m.exit:
		return nil
	default:
		if !m.finalized {
			if err := m.Finalize(); err != nil {
				u.Errorf("Could not finalize: %v", err)
				return nil
			}
		}
		if m.cursor >= len(m.Vals) {
			return nil
		}
		m.cursor++
		//u.Debugf("ResultReader.Next():  cursor:%v  %v", m.cursor, len(m.Vals[m.cursor-1]))
		return models.ValsMessage{m.Vals[m.cursor-1], uint64(m.cursor)}
	}
}
