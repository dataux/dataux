package lytics

import (
	"database/sql/driver"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/rel"
	"github.com/araddon/qlbridge/value"
)

var (
	// Ensure we implement Task
	_ exec.Task = (*ResultReader)(nil)
)

// ResultReader, adapts the lytics http json response
type ResultReader struct {
	*exec.TaskBase
	finalized     bool
	hasprojection bool
	cursor        int
	proj          *rel.Projection
	Docs          []u.JsonHelper
	Vals          [][]driver.Value
	cols          []string
	Total         int
	Aggs          u.JsonHelper
	ScrollId      string
	Req           *Generator
}

// A wrapper, allowing us to implement sql/driver Next() interface
//   which is different than qlbridge/datasource Next()
type ResultReaderNext struct {
	*ResultReader
}

func NewResultReader(req *Generator) *ResultReader {
	m := &ResultReader{}
	m.TaskBase = exec.NewTaskBase(req.ctx)
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
			//u.Infof("found %#v", fld)
			cols = append(cols, rel.NewResultColumn(fld.Name, len(cols), nil, fld.Type))
		}
	} else if sql.CountStar() {
		// Count *
		cols = append(cols, rel.NewResultColumn("count", len(cols), nil, value.IntType))
	} else if len(m.Aggs) > 0 {

	} else {
		for _, col := range m.Req.sel.Columns {
			if fld, ok := m.Req.tbl.FieldMap[col.SourceField]; ok {
				//u.Debugf("column: %#v", col)
				cols = append(cols, rel.NewResultColumn(col.SourceField, len(cols), col, fld.Type))
			} else {
				if fld, ok := m.Req.tbl.FieldMap[col.As]; ok {
					//u.Debugf("column: %#v", col)
					cols = append(cols, rel.NewResultColumn(col.SourceField, len(cols), col, fld.Type))
				} else {
					//u.Debugf("col %#v", col)
					//u.Debugf("Could not find: %v  sourcefield?%v  fields:%v", col.As, col.SourceField, m.Req.tbl.Columns())
					cols = append(cols, rel.NewResultColumn(col.SourceField, len(cols), col, value.StringType))
				}
			}
		}
	}
	m.proj.Columns = cols
	colNames := make([]string, len(cols))
	for i, col := range cols {
		colNames[i] = col.Name
	}
	m.cols = colNames
	u.Debugf("leaving Columns:  %v", len(m.proj.Columns))
}

func (m *ResultReader) Columns() []string {
	m.buildProjection()
	return m.cols
}

// Run() Fetch api response
func (m *ResultReader) Run() error {

	sigChan := m.SigChan()
	outCh := m.MessageOut()

	m.finalized = true
	m.buildProjection()

	defer func() {
		close(outCh) // closing output channels is the signal to stop
		u.Debugf("nice, finalize ResultReader out: %p  row ct %v", outCh, len(m.Vals))
	}()

	//sql := m.Req.sel

	m.Vals = make([][]driver.Value, 0)
	colNames := make(map[string]int, len(m.proj.Columns))
	for i, col := range m.proj.Columns {
		colNames[col.As] = i
	}

	for i, vals := range m.Vals {
		//u.Debugf("new row ct: %v cols:%v vals:%v", len(m.Vals), colNames, vals)
		//msg := &datasource.SqlDriverMessage{vals, len(m.Vals)}
		msg := datasource.NewSqlDriverMessageMap(uint64(i), vals, colNames)
		//u.Infof("In source Scanner iter %#v", msg)
		select {
		case <-sigChan:
			return nil
		case outCh <- msg:
		}
	}

	return nil
}
