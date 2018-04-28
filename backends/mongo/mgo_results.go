package mongo

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/value"
)

var (
	// Ensure we implement TaskRunner
	_ exec.TaskRunner = (*ResultReader)(nil)
)

// ResultReader Mongo implements result paging, reading
// - driver.Rows
type ResultReader struct {
	*exec.TaskBase
	finalized bool
	cursor    int
	limit     int
	Docs      []u.JsonHelper
	Vals      [][]driver.Value
	Total     int
	Aggs      u.JsonHelper
	ScrollId  string
	query     *mgo.Query
	sql       *SqlToMgo
}

// ResultReaderNext a wrapper, allowing us to implement sql/driver Next() interface
// which is different than qlbridge/datasource Next()
type ResultReaderNext struct {
	*ResultReader
}

func NewResultReader(req *SqlToMgo, q *mgo.Query, limit int) *ResultReader {
	m := &ResultReader{}
	if req.Ctx == nil {
		u.Errorf("no context? %p", m)
	}
	m.TaskBase = exec.NewTaskBase(req.Ctx)
	m.query = q
	//u.LogTraceDf(u.WARN, 16, "hello")
	//u.Debugf("new resultreader:  sqltomgo:%p   sourceplan:%p", req, req.p)
	m.sql = req
	m.limit = limit
	return m
}

func (m *ResultReader) Close() error {
	return nil
}

func (m *ResultReader) Run() error {
	sigChan := m.SigChan()
	outCh := m.MessageOut()
	//defer context.Recover()
	defer func() {
		close(outCh) // closing output channels is the signal to stop
		//m.TaskBase.Close()
	}()

	m.finalized = true

	sql := m.sql.sel
	if sql == nil {
		u.Warnf("no sql? %p  %#v", m.sql, m.sql)
		return fmt.Errorf("no sql")
	}
	if m.sql.p == nil {
		u.Warnf("no plan????  %#v", m.sql)
		return fmt.Errorf("no plan")
	}
	//cols := m.sql.sel.Columns
	//u.Debugf("m.sql %p %#v", m.sql, m.sql)
	if m.sql == nil {
		return fmt.Errorf("No sqltomgo?????? ")
	}
	//u.Debugf("%p about to blow up sqltomgo: %p", m.sql.p, m.sql)
	if m.sql.p == nil {
		u.Warnf("no plan?")
		return fmt.Errorf("no plan? %v", m.sql)
	}
	if m.sql.p.Proj == nil {
		u.Warnf("no projection?? %#v", m.sql.p)
		return fmt.Errorf("no plan? %v", m.sql)
	}
	cols := m.sql.p.Proj.Columns
	colNames := make(map[string]int, len(cols))
	if m.sql.needsPolyFill {
		// since we are asking for poly-fill, the col names
		// are not projected
		for i, col := range cols {
			colNames[col.Col.SourceField] = i
		}
	} else {
		for i, col := range cols {
			colNames[col.As] = i
		}
	}

	//u.Debugf("sqltomgo:%p  resultreader:%p colnames? %v", m.sql, m, colNames)

	if sql.CountStar() {
		// select count(*)
		vals := make([]driver.Value, 1)
		ct, err := m.query.Count()
		if err != nil {
			u.Errorf("could not get count(*) from mongo: %v", err)
			return err
		}
		// we are going to write the count as a string?  whatevers mysql.
		vals[0] = fmt.Sprintf("%d", ct)
		m.Vals = append(m.Vals, vals)
		//u.Debugf("was a select count(*) query %d", ct)
		msg := datasource.NewSqlDriverMessageMap(uint64(1), vals, colNames)
		//u.Debugf("In source Scanner iter %#v", msg)
		outCh <- msg

		return nil
	}

	if m.limit != 0 {
		m.query = m.query.Limit(m.limit)
	}

	if len(cols) == 0 {
		u.Errorf("WTF?  no cols? %v", cols)
	}

	//n := time.Now()
	m.Vals = make([][]driver.Value, 0)
	iter := m.query.Iter()
	for {
		var bm bson.M
		if !iter.Next(&bm) {
			break
		}
		vals := make([]driver.Value, len(cols))
		for i, col := range cols {
			if val, ok := bm[col.SourceName()]; ok {
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
					// u.Warnf("col=%d  ? %v type=%q  T=%T", i, col, col.Type.String(), vt)
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
		//u.Debugf("mongo result msg out %#v", msg)
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
	//u.Debugf("finished query, took: %v for %v rows", time.Now().Sub(n), len(m.Vals))
	return nil
}
