package bigquery

import (
	"database/sql/driver"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	u "github.com/araddon/gou"
	"golang.org/x/net/context"
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
	Req           *SqlToBQ
}

// A wrapper, allowing us to implement sql/driver Next() interface
//   which is different than qlbridge/datasource Next()
type ResultReaderNext struct {
	*ResultReader
}

func NewResultReader(req *SqlToBQ) *ResultReader {
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

// Runs the Google BigQuery job
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

	// tbl, err := m.Req.s.Table(colFamily)
	// if err != nil {
	// 	u.Warnf("could not find schema column %v", err)
	// 	return err
	// }

	cols := m.Req.p.Proj.Columns
	colNames := make(map[string]int, len(cols))
	for i, col := range cols {
		colNames[col.As] = i
		//u.Debugf("col.name=%v  col.as=%s", col.Name, col.As)
	}
	if len(cols) == 0 {
		u.Errorf("no cols? %v  *?", cols)
	}

	limit := DefaultLimit
	if m.Req.sel.Limit > 0 {
		limit = m.Req.sel.Limit
	}

	u.Debugf("%p bq limit: %d sel:%s", m.Req.sel, limit, sel)
	queryStart := time.Now()

	// qry := fmt.Sprintf(`SELECT name, count FROM [%s.%s]`, m.Req.s.dataset, tableID)
	client, err := bigquery.NewClient(context.Background(), m.Req.s.billingProject)
	if err != nil {
		u.Warnf("Could not create bigquery client billing_project=%q  err=%v", m.Req.s.billingProject, err)
		return err
	}
	q := client.Query(sel.String())

	ctx := context.Background()
	job, err := q.Run(ctx)
	if err != nil {
		u.Warnf("could not run %v", err)
		return err
	}

	// Wait until async querying is done.
	status, err := job.Wait(ctx)
	if err != nil {
		u.Warnf("could not run %v", err)
		return err
	}
	if err := status.Err(); err != nil {
		u.Warnf("could not run %v", err)
		return err
	}

	// u.Debugf("Status state:%#v stats:%#v", status.State, status.Statistics)

	it, err := job.Read(ctx)
	for {
		var row []bigquery.Value
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}
		//u.Debugf("row %#v", row)

		vals := make([]driver.Value, len(cols))
		for i, v := range row {
			switch v := v.(type) {
			case civil.Date:
				vals[i] = v.In(time.UTC)
			default:
				vals[i] = v
			}
			//col := cols[i]
			//u.Debugf("%-2d col.as=%-10s prop.T %T\tprop.v=%v", i, col.As, v, v)
		}

		//u.Debugf("new row ct: %v cols:%v vals:%v", m.Total, colNames, vals)
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
	}

	u.Debugf("finished query, took: %v for %v rows", time.Now().Sub(queryStart), m.Total)
	return nil
}
