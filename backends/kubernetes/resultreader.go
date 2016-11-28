package kubernetes

import (
	"fmt"
	"time"

	u "github.com/araddon/gou"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"

	//"k8s.io/client-go/1.4/kubernetes"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/rel"
	"github.com/araddon/qlbridge/value"
)

var (
	// Ensure we implement TaskRunner
	_ exec.TaskRunner = (*ResultReader)(nil)
)

// ResultReader implements result paging, reading of the json/grpc
// api responses from kube
type ResultReader struct {
	*exec.TaskBase
	exit          <-chan bool
	finalized     bool
	hasprojection bool
	cursor        int
	proj          *rel.Projection
	cols          []string
	Total         int
	req           *SqlToKube
}

func NewResultReader(req *SqlToKube) *ResultReader {
	m := &ResultReader{}
	m.TaskBase = exec.NewTaskBase(req.Ctx)
	m.req = req
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
	sql := m.req.sel
	if sql.Star {
		// Select Each field, grab fields from Table Schema
		for _, fld := range m.req.tbl.Fields {
			cols = append(cols, rel.NewResultColumn(fld.Name, len(cols), nil, fld.Type))
		}
	} else if sql.CountStar() {
		// Count *
		cols = append(cols, rel.NewResultColumn("count", len(cols), nil, value.IntType))
	} else {
		for _, col := range m.req.sel.Columns {
			if fld, ok := m.req.tbl.FieldMap[col.SourceField]; ok {
				//u.Debugf("column: %#v", col)
				cols = append(cols, rel.NewResultColumn(col.SourceField, len(cols), col, fld.Type))
			} else {
				u.Debugf("Could not find: '%v' in %#v", col.SourceField, m.req.tbl.FieldMap)
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
	queryStart := time.Now()
	defer func() {
		close(outCh) // closing output channels is the signal to stop
		u.Debugf("nice, finalize ResultReader out: %p  row ct %v", outCh, m.Total)
		u.Infof("finished query, took: %v for %v rows", time.Now().Sub(queryStart), m.Total)
	}()

	m.finalized = true
	m.buildProjection()
	sel := m.req.sel

	if len(sel.From) > 1 {
		froms := make([]string, 0, len(sel.From))
		for _, f := range sel.From {
			froms = append(froms, f.Name)
		}
		return fmt.Errorf("Only 1 source supported on Kubernetes queries but got %v", froms)
	}

	if sel.CountStar() {
		u.Warnf("Count(*) on Kubernetes")
	}

	colFamily := sel.From[0].Name
	tbl, err := m.req.s.Table(colFamily)
	if err != nil {
		u.Warnf("could not find schema column %v", err)
		return err
	}

	colMap := tbl.FieldNamesPositions()
	//u.Debugf("tbl %p  %v", tbl, tbl)

	// Kube doesn't currently support paging 11/2016
	// limit := DefaultLimit
	// if sel.Limit > 0 {
	// 	limit = sel.Limit
	// }

	//u.Debugf("%p limit: %d sel:%s", sel, limit, sel)

	iter := Objects(context.Background(), tbl.Name, m.req)

	for {
		o, err := iter.Next()
		switch err {
		case nil:
			//u.Debugf("result row: %v cols:%v vals:%v", m.Total, colMap, o.row)
			msg := datasource.NewSqlDriverMessageMap(uint64(m.Total), o.row, colMap)
			m.Total++
			select {
			case <-sigChan:
				return nil
			case outCh <- msg:
				// continue
			}
		case iterator.Done:
			return nil
		default:
			u.Warnf("error listing %s err:%v", tbl.Name, err)
			return err
		}
	}

	return nil
}
