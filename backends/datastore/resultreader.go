package datastore

import (
	"database/sql/driver"
	"time"

	"cloud.google.com/go/datastore"
	u "github.com/araddon/gou"
	"google.golang.org/api/iterator"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/rel"
	"github.com/araddon/qlbridge/value"
)

var (
	// Ensure we implement TaskRunner
	_ exec.TaskRunner = (*ResultReader)(nil)
)

// ResultReader for Google Datastore implements result paging, reading
// from datastore types into generic sql types
// - driver.Rows
type ResultReader struct {
	*exec.TaskBase
	exit          <-chan bool
	finalized     bool
	hasprojection bool
	cursor        int
	proj          *rel.Projection
	cols          []string
	Vals          [][]driver.Value
	Total         int
	Req           *SqlToDatstore
}

// ResultReaderNext A wrapper, allowing us to implement sql/driver Next() interface
//   which is different than qlbridge/datasource Next()
type ResultReaderNext struct {
	*ResultReader
}

func NewResultReader(req *SqlToDatstore) *ResultReader {
	m := &ResultReader{}
	m.TaskBase = exec.NewTaskBase(req.Ctx)
	m.Req = req
	return m
}

func (m *ResultReader) Close() error { return nil }

func (m *ResultReader) buildProjection() {

	if m.hasprojection {
		return
	}
	m.hasprojection = true
	m.proj = rel.NewProjection()
	cols := m.proj.Columns
	sql := m.Req.sel
	if sql.Star {
		// Select Each field, grab fields from Table Schema
		for _, fld := range m.Req.tbl.Fields {
			cols = append(cols, rel.NewResultColumn(fld.Name, len(cols), nil, fld.Type))
		}
	} else if sql.CountStar() {
		// Count *
		cols = append(cols, rel.NewResultColumn("count", len(cols), nil, value.IntType))
	} else {
		for _, col := range m.Req.sel.Columns {
			if fld, ok := m.Req.tbl.FieldMap[col.SourceField]; ok {
				//u.Debugf("column: %#v", col)
				cols = append(cols, rel.NewResultColumn(col.SourceField, len(cols), col, fld.Type))
			} else {
				u.Debugf("Could not find: '%v' in %#v", col.SourceField, m.Req.tbl.FieldMap)
				u.Warnf("%#v", col)
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

// Run takes sql query which has been translated into a
// google datastore the Google Datastore properties into
//    [][]interface{}   which is compabitble with sql/driver values
// as well as making a projection, ie column selection
func (m *ResultReader) Run() error {

	sigChan := m.SigChan()
	outCh := m.MessageOut()
	//defer context.Recover()
	defer func() {
		close(outCh) // closing output channels is the signal to stop
		//m.TaskBase.Close()
		u.Debugf("finalize ResultReader out: %p  row ct %v", outCh, len(m.Vals))
	}()
	m.finalized = true
	m.buildProjection()

	sql := m.Req.sel
	//u.Infof("query: %#v", m.Req.dsq)
	q := m.Req.dsq

	m.Vals = make([][]driver.Value, 0)

	if sql.CountStar() {
		// Count(*)
		vals := make([]driver.Value, 1)
		ct, err := m.Req.dsClient.Count(m.Req.dsCtx, q)
		//ct, err := q.Count(m.Req.dsCtx)
		if err != nil {
			u.Errorf("could not get count: %v", err)
			return err
		}
		vals[0] = ct
		m.Vals = append(m.Vals, vals)
		return nil
	}

	cols := m.Req.p.Proj.Columns
	colNames := make(map[string]int, len(cols))
	for i, col := range cols {
		colNames[col.Name] = i
		//u.Debugf("col.name=%v  col.as=%s", col.Name, col.As)
	}
	needsProjection := false
	posMap := make([]int, len(cols))
	if len(cols) == 0 {
		u.Errorf("no cols? %v  *?", cols)
	} else {
		// Doing projection on server side datastore really seems to
		// introduce all kinds of weird errors and rules, so mostly just
		// do projection here in proxy

		needsProjection = true
		for i, col := range cols {
			for fieldIndex, fld := range m.Req.tbl.Fields {
				//u.Debugf("name: %v orig:%v   name:%v", fld.Name, fieldIndex, col.Name)
				if col.Name == fld.Name {
					posMap[i] = fieldIndex
					break
				}
			}
		}
	}
	if m.Req.sel.Limit > 0 {
		//u.Infof("setting limit: %v", m.Req.sel.Limit)
		q = q.Limit(m.Req.sel.Limit)
	}
	queryStart := time.Now()
	// u.Debugf("posMap: %#v", posMap)
	// for fieldIndex, fld := range m.Req.tbl.Fields {
	// 	u.Debugf("field name: %v orig:%v  ", fld.Name, fieldIndex)
	// }
	// for i, col := range cols {
	// 	u.Debugf("col? %v:%v", i, col)
	// }
	//u.Infof("datastore query: %v", q)
	//q.DebugInfo()
	//iter := q.Run(m.Req.dsCtx)
	//u.Infof("has:  client? %v  ctx? %v", m.Req.dsClient, m.Req.dsCtx)
	iter := m.Req.dsClient.Run(m.Req.dsCtx, q)
	for {
		row := Row{}
		key, err := iter.Next(&row)
		if err != nil {
			if err == iterator.Done {
				u.Infof("done? rowct=%v", len(m.Vals))
				break
			}
			u.Errorf("uknown datastore error: %v", err)
			return err
		}

		row.key = key
		var vals []driver.Value
		if needsProjection {

			vals = make([]driver.Value, len(cols))
			for _, prop := range row.props {
				//found := false
				for i, col := range cols {
					//u.Infof("prop.name=%s col.Name=%s", prop.Name, col.Name)
					if col.Name == prop.Name {
						vals[i] = prop.Value
						//u.Debugf("%-2d col.name=%-10s prop.T %T\tprop.v%v", i, col.Name, prop.Value, prop.Value)
						//found = true
						break
					}
				}
				// if !found {
				// 	u.Warnf("not found? %#v", prop)
				// }
			}

			//u.Debugf("%v", vals)
			//vals = row.Vals
			m.Vals = append(m.Vals, vals)
		} else {
			vals = row.Vals(colNames)
			m.Vals = append(m.Vals, vals)
		}
		u.Debugf("new row ct: %v cols:%v vals:%v", len(m.Vals), colNames, vals)
		//msg := &datasource.SqlDriverMessage{vals, len(m.Vals)}
		msg := datasource.NewSqlDriverMessageMap(uint64(len(m.Vals)), vals, colNames)
		//u.Debugf("In gds source iter %#v", vals)
		select {
		case <-sigChan:
			return nil
		case outCh <- msg:
			// continue
		}
		//u.Debugf("vals:  %v", row.Vals)
	}
	u.Infof("finished query, took: %v for %v rows", time.Now().Sub(queryStart), len(m.Vals))
	return nil
}

func pageRowsQuery(iter *datastore.Iterator) []Row {
	rows := make([]Row, 0)
	for {
		row := Row{}
		if key, err := iter.Next(&row); err != nil {
			if err == iterator.Done {
				break
			}
			u.Errorf("error: %v", err)
			break
		} else {
			row.key = key
			//u.Debugf("key:  %#v", key)
			rows = append(rows, row)
		}
	}
	return rows
}

type Row struct {
	vals  []driver.Value
	props []datastore.Property
	key   *datastore.Key
}

func (m *Row) Vals(cols map[string]int) []driver.Value {
	if len(m.vals) > 0 {
		return m.vals
	}
	m.vals = make([]driver.Value, len(cols))
	for colName, idx := range cols {
		for _, p := range m.props {
			if p.Name == colName {
				//u.Infof("%d prop: %#v", idx, p)
				m.vals[idx] = p.Value
			}
		}

	}
	return m.vals
}
func (m *Row) Load(props []datastore.Property) error {
	//m.Vals = make([]driver.Value, len(props))
	m.props = props
	//u.Infof("Load: %#v", props)
	// for i, p := range props {
	// 	u.Infof("%d prop: %#v", i, p)
	// 	m.Vals[i] = p.Value
	// }
	return nil
}
func (m *Row) Save() ([]datastore.Property, error) {
	return nil, nil
}
