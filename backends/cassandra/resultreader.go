package cassandra

import (
	"database/sql/driver"
	"time"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/rel"
	"github.com/araddon/qlbridge/value"
)

var (
	// Ensure we implement TaskRunner
	_ exec.TaskRunner = (*ResultReader)(nil)
)

func NewCassDialect() expr.DialectWriter {
	return expr.NewDialectWriter('\'', '"')
}

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
	Req           *SqlToCql
}

// A wrapper, allowing us to implement sql/driver Next() interface
//   which is different than qlbridge/datasource Next()
type ResultReaderNext struct {
	*ResultReader
}

func NewResultReader(req *SqlToCql) *ResultReader {
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

// Runs the Google Datastore properties into
//    [][]interface{}   which is compabitble with sql/driver values
// as well as making a projection, ie column selection
//
// func (m *ResultReader) Finalize() error {
func (m *ResultReader) Run() error {

	sigChan := m.SigChan()
	outCh := m.MessageOut()
	defer func() {
		close(outCh) // closing output channels is the signal to stop
		u.Debugf("nice, finalize ResultReader out: %p  row ct %v", outCh, m.Total)
	}()
	m.finalized = true
	m.buildProjection()

	if m.Req.sel.CountStar() {
		// Count(*) Do we really want to do push-down here?
		//  possibly default to not allowing this and allow a setting?
		u.Warnf("Count(*) on Cassandra, your crazy!")
	}

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

	sel := m.Req.sel

	u.Debugf("%p cass limit: %d sel:%s", m.Req.sel, limit, sel)
	queryStart := time.Now()

	cassWriter := NewCassDialect()
	sel.WriteDialect(cassWriter)
	cqlQuery := cassWriter.String()
	cassQry := m.Req.s.session.Query(cqlQuery).PageSize(limit)
	iter := cassQry.Iter()

	for {
		row := make(map[string]interface{})
		if !iter.MapScan(row) {
			//u.Debugf("done with query")
			break
		}

		vals := make([]driver.Value, len(cols))
		for k, v := range row {
			found := false
			for i, col := range cols {
				//u.Infof("prop.name=%s col.Name=%s", prop.Name, col.Name)
				if col.Name == k {
					vals[i] = v
					//u.Debugf("%-2d col.name=%-10s prop.T %T\tprop.v%v", i, col.Name, v, v)
					found = true
					break
				}
			}
			if !found {
				u.Warnf("not found? %s=%v", k, v)
			}
		}

		u.Debugf("new row ct: %v cols:%v vals:%v", m.Total, colNames, vals)
		//msg := &datasource.SqlDriverMessage{vals, len(m.Vals)}
		msg := datasource.NewSqlDriverMessageMap(uint64(m.Total), vals, colNames)
		m.Total++
		//u.Debugf("In gds source iter %#v", vals)
		select {
		case <-sigChan:
			return nil
		case outCh <- msg:
			// continue
		}
		//u.Debugf("vals:  %v", row.Vals)
	}
	err := iter.Close()
	//if err != nil {
	//u.Errorf("could not close iter %T err:%v", err, err)
	//}
	u.Infof("finished query, took: %v for %v rows err:%v", time.Now().Sub(queryStart), m.Total, err)
	return nil
}
