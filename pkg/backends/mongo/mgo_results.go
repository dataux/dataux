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
	_ exec.TaskRunner = (*ResultReader)(nil)
	//_ datasource.SchemaColumns = (*ResultReader)(nil)
	//_ datasource.Scanner       = (*ResultReader)(nil)
)

// Mongo ResultReader implements result paging, reading
// - driver.Rows
type ResultReader struct {
	*exec.TaskBase
	finalized bool
	cursor    int
	Docs      []u.JsonHelper
	Vals      [][]driver.Value
	Total     int
	Aggs      u.JsonHelper
	ScrollId  string
	query     *mgo.Query
	sql       *SqlToMgo
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
	m.sql = req
	return m
}

func (m *ResultReader) Close() error { return nil }
func (m *ResultReader) MesgChan(filter expr.Node) <-chan datasource.Message {
	iter := m.CreateIterator(filter)
	return datasource.SourceIterChannel(iter, filter, m.SigChan())
}
func (m *ResultReader) CreateIterator(filter expr.Node) datasource.Iterator {
	return &ResultReaderNext{m}
}

func (m *ResultReader) Run(context *expr.Context) error {
	sigChan := m.SigChan()
	outCh := m.MessageOut()
	//defer context.Recover()
	defer func() {
		close(outCh) // closing output channels is the signal to stop
		//m.TaskBase.Close()
		u.Debugf("nice, finalize ResultReader out: %p  row ct %v", outCh, len(m.Vals))
	}()

	m.finalized = true

	//u.LogTracef(u.WARN, "hello")

	sql := m.sql.sel

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

	//cols := m.sql.sel.Columns
	cols := m.sql.sp.Proj.Columns
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
		u.Debugf("col? %v", bm)
		vals := make([]driver.Value, len(cols))
		for i, col := range cols {
			if val, ok := bm[col.Col.SourceField]; ok {
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
		//u.Debugf("new row ct: %v cols:%v vals:%v", len(m.Vals), colNames, vals)
		//msg := &datasource.SqlDriverMessage{vals, len(m.Vals)}
		msg := datasource.NewSqlDriverMessageMap(uint64(len(m.Vals)), vals, colNames)
		//u.Infof("In source Scanner iter %#v", msg)
		select {
		case <-sigChan:
			return nil
		case outCh <- msg:
			// continue
		}
	}
	//u.Debugf("about to close")
	if err := iter.Close(); err != nil {
		u.Errorf("could not iter: %v", err)
		return err
	}
	u.Debugf("finished query, took: %v for %v rows", time.Now().Sub(n), len(m.Vals))
	return nil
}
func (m *ResultReader) Finalize() error { return nil }

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
	case <-m.SigChan():
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
