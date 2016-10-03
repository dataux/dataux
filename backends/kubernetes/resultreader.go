package kubernetes

import (
	"fmt"
	"time"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/rel"
	"github.com/araddon/qlbridge/value"
)

var (
	// Ensure we implement TaskRunner
	_ exec.TaskRunner = (*ResultReader)(nil)
)

// ResultReader implements result paging, reading
type ResultReader struct {
	*exec.TaskBase
	exit          <-chan bool
	finalized     bool
	hasprojection bool
	cursor        int
	proj          *rel.Projection
	cols          []string
	Total         int
	Req           *SqlToKube
}

// A wrapper, allowing us to implement sql/driver Next() interface
//   which is different than qlbridge/datasource Next()
type ResultReaderNext struct {
	*ResultReader
}

func NewResultReader(req *SqlToKube) *ResultReader {
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

// Runs the Kubernetes Result exec
func (m *ResultReader) Run() error {

	sigChan := m.SigChan()
	outCh := m.MessageOut()
	defer func() {
		close(outCh) // closing output channels is the signal to stop
		u.Debugf("nice, finalize ResultReader out: %p  row ct %v", outCh, m.Total)
	}()
	m.finalized = true
	m.buildProjection()
	sel := m.Req.sel

	if len(sel.From) > 1 {
		froms := make([]string, 0, len(sel.From))
		for _, f := range sel.From {
			froms = append(froms, f.Name)
		}
		return fmt.Errorf("Only 1 source supported on Kubernetes queries got %v", froms)
	}

	if sel.CountStar() {
		u.Warnf("Count(*) on Kubernetes, your crazy!")
	}

	colFamily := sel.From[0].Name

	tbl, err := m.Req.s.Table(colFamily)
	if err != nil {
		u.Warnf("could not find schema column %v", err)
		return err
	}
	u.Debugf("tbl %v", tbl)

	cols := m.Req.p.Proj.Columns
	colNames := make(map[string]int, len(cols))
	for i, col := range cols {
		colNames[col.Name] = i
		//u.Debugf("col.name=%v  col.as=%s", col.Name, col.As)
	}
	if len(cols) == 0 {
		u.Errorf("no cols? %v  *?", cols)
	}

	limit := DefaultLimit
	if m.Req.sel.Limit > 0 {
		limit = m.Req.sel.Limit
	}

	u.Debugf("%p bt limit: %d sel:%s", m.Req.sel, limit, sel)
	queryStart := time.Now()

	//u.Debugf("%s new row ct: %v cols:%v vals:%v", r.Key(), m.Total, colNames, vals)
	//msg := &datasource.SqlDriverMessage{vals, len(m.Vals)}
	msg := datasource.NewSqlDriverMessageMap(uint64(m.Total), nil, colNames)
	m.Total++
	//u.Debugf("In gds source iter %#v", vals)
	select {
	case <-sigChan:
		return nil
	case outCh <- msg:
		// continue
	}

	u.Infof("finished query, took: %v for %v rows", time.Now().Sub(queryStart), m.Total)
	return nil
}
