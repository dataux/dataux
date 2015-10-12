package mongo

import (
	"database/sql/driver"
	"encoding/json"
	"io"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/exec"
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

// Mongo ResultReader implements result paging, reading
// - driver.Rows
type ResultReader struct {
	*exec.TaskBase
	exit          <-chan bool
	finalized     bool
	hasprojection bool
	cursor        int
	proj          *expr.Projection
	cols          []string
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
	m.TaskBase = exec.NewTaskBase("mgo-resultreader")
	m.query = q
	m.Req = req
	// m.TaskBase.Handler = func(ctx *expr.Context, msg datasource.Message) bool {
	// 	switch mt := msg.(type) {
	// 	case *datasource.SqlDriverMessage:
	// 		u.Debugf("Result:  T:%T  vals:%#v", msg, mt.Vals)
	// 	case nil:
	// 		u.Warnf("got nil")
	// 		// Signal to quit
	// 		return false

	// 	default:
	// 		u.Errorf("could not convert to message reader: %T", msg)
	// 	}

	// 	return true
	// }
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
	} else {
		for _, col := range m.Req.sel.Columns {
			if fld, ok := m.Req.tbl.FieldMap[col.SourceField]; ok {
				//u.Debugf("column: %#v", col)
				cols = append(cols, expr.NewResultColumn(col.SourceField, len(cols), col, fld.Type))
			} else {
				u.Debugf("Could not find: '%v' in %#v", col.SourceField, m.Req.tbl.FieldMap)
				//u.Warnf("%#v", col)
			}
		}
	}
	colNames := make([]string, len(cols))
	for i, col := range cols {
		colNames[i] = col.As
	}
	m.cols = colNames
	m.proj.Columns = cols
	//u.Debugf("leaving Columns:  %v", len(m.proj.Columns))
}

func (m *ResultReader) Tables() []string {
	return nil
}

func (m *ResultReader) Columns() []string {
	return m.cols
}

func (m *ResultReader) Projection() (*expr.Projection, error) {
	m.buildProjection()
	return m.proj, nil
}

func (m *ResultReader) Open(connInfo string) (datasource.SourceConn, error) {
	panic("Not implemented")
	return m, nil
}

func (m *ResultReader) Schema() *datasource.Schema {
	return m.Req.tbl.Schema
}

func (m *ResultReader) MesgChan(filter expr.Node) <-chan datasource.Message {
	iter := m.CreateIterator(filter)
	return datasource.SourceIterChannel(iter, filter, m.exit)
}

func (m *ResultReader) CreateIterator(filter expr.Node) datasource.Iterator {
	return &ResultReaderNext{m}
}

func (m *ResultReader) Run(context *expr.Context) error {
	defer context.Recover()
	defer func() {
		m.TaskBase.Close()
		u.Debugf("nice, finalize vals in ResultReader: %v", len(m.Vals))
	}()

	sigChan := m.SigChan()
	outCh := m.MessageOut()

	m.finalized = true
	m.buildProjection()

	//u.LogTracef(u.WARN, "hello")

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
	colNames := make(map[string]int, len(cols))
	for i, col := range cols {
		colNames[col.As] = i
	}
	if len(cols) == 0 {
		u.Errorf("WTF?  no cols? %v", cols)
	}

	n := time.Now()
	iter := m.query.Iter()
	for {
		var bm bson.M
		if !iter.Next(&bm) {
			break
		}
		//u.Debugf("col? %v", bm)
		vals := make([]driver.Value, len(cols))
		for i, col := range cols {
			if val, ok := bm[col.Name]; ok {
				switch vt := val.(type) {
				case bson.ObjectId:
					vals[i] = vt.Hex()
				case bson.M, bson.D:
					by, err := json.Marshal(vt)
					if err != nil {
						u.Warnf("could not convert bson -> json: %v  for %#v", err, vt)
						vals[i] = make([]byte, 0)
					} else {
						vals[i] = by
					}
				default:
					vals[i] = vt
				}

			} else {
				// Not returned in query, sql hates missing fields
				// Should we zero/empty fill here or in mysql handler?
				if col.Type == value.StringType {
					vals[i] = ""
				}
			}
		}
		m.Vals = append(m.Vals, vals)
		u.Debugf("new row ct: %v", len(m.Vals))
		//msg := &datasource.SqlDriverMessage{vals, len(m.Vals)}
		msg := datasource.NewSqlDriverMessageMap(uint64(len(m.Vals)), vals, colNames)
		u.Infof("In source Scanner iter %#v", msg)
		select {
		case <-sigChan:
			return nil
		case outCh <- msg:
			// continue
		}
	}
	if err := iter.Close(); err != nil {
		u.Errorf("could not iter: %v", err)
		return err
	}
	u.Infof("finished query, took: %v for %v rows", time.Now().Sub(n), len(m.Vals))
	return nil
}
func (m *ResultReader) Finalize() error { return nil }

// Finalize maps the Mongo Documents/results into
//    [][]interface{}   which is compabitble with sql/driver values
//
func (m *ResultReader) XXXFinalize() error {

	m.finalized = true
	m.buildProjection()

	u.LogTracef(u.WARN, "hello")
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

	n := time.Now()
	iter := m.query.Iter()
	for {
		var bm bson.M
		if !iter.Next(&bm) {
			break
		}
		//u.Debugf("col? %v", bm)
		vals := make([]driver.Value, len(cols))
		for i, col := range cols {
			if val, ok := bm[col.Name]; ok {
				switch vt := val.(type) {
				case bson.ObjectId:
					vals[i] = vt.Hex()
				case bson.M, bson.D:
					by, err := json.Marshal(vt)
					if err != nil {
						u.Warnf("could not convert bson -> json: %v  for %#v", err, vt)
						vals[i] = make([]byte, 0)
					} else {
						vals[i] = by
					}
				default:
					vals[i] = vt
				}

			} else {
				// Not returned in query, sql hates missing fields
				// Should we zero/empty fill here or in mysql handler?
				if col.Type == value.StringType {
					vals[i] = ""
				}
			}
		}
		m.Vals = append(m.Vals, vals)
		u.Debugf("new row ct: %v", len(m.Vals))
	}
	if err := iter.Close(); err != nil {
		u.Errorf("could not iter: %v", err)
		return err
	}
	u.Infof("finished query, took: %v for %v rows", time.Now().Sub(n), len(m.Vals))
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
		return &datasource.SqlDriverMessage{m.Vals[m.cursor-1], uint64(m.cursor)}
	}
}
