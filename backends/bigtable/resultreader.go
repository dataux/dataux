package bigtable

import (
	"database/sql/driver"
	"fmt"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/bigtable"
	"github.com/araddon/dateparse"
	u "github.com/araddon/gou"
	"golang.org/x/net/context"

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
	Req           *SqlToBT
}

// A wrapper, allowing us to implement sql/driver Next() interface
//   which is different than qlbridge/datasource Next()
type ResultReaderNext struct {
	*ResultReader
}

func NewResultReader(req *SqlToBT) *ResultReader {
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

// Runs the Google BigTable exec
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

	if sel.CountStar() {
		// Count(*) Do we really want to do push-down here?
		//  possibly default to not allowing this and allow via setting?
		u.Warnf("Count(*) on BigTable, your crazy!")
	}

	colFamily := ""
	if len(sel.From) > 1 {
		froms := make([]string, 0, len(sel.From))
		for _, f := range sel.From {
			froms = append(froms, f.Name)
		}
		return fmt.Errorf("Only 1 source supported on BigTable queries got %v", froms)
	}

	colFamily = sel.From[0].Name
	colFamilyPrefix := colFamily + ":"

	tbl, err := m.Req.s.Table(colFamily)
	if err != nil {
		u.Warnf("could not find schema column %v", err)
		return err
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

	u.Debugf("%p bt limit: %d sel:%s", m.Req.sel, limit, sel)
	queryStart := time.Now()

	ctx := context.Background()
	btt := m.Req.s.client.Open(tbl.Parent)
	var rr bigtable.RowRange
	var opts []bigtable.ReadOption
	//opts = append(opts, bigtable.LimitRows(20))
	f := bigtable.ChainFilters(
		bigtable.FamilyFilter(colFamily),
		bigtable.LatestNFilter(1),
	)
	opts = append(opts, bigtable.RowFilter(f))

	err = btt.ReadRows(ctx, rr, func(r bigtable.Row) bool {
		//printRow(r)
		//row := make(map[string]interface{})

		rcells := r[colFamily]
		//sort.Sort(byColumn(rcells))

		vals := make([]driver.Value, len(cols))
		for _, v := range rcells {
			found := false
			key := strings.Replace(v.Column, colFamilyPrefix, "", 1)
			for i, col := range cols {
				//u.Infof("%s col.Name=%s", key, col.Name)
				if col.Name == key {
					switch col.Type {
					case value.StringType:
						vals[i] = string(v.Value)
					case value.BoolType:
						bv, err := strconv.ParseBool(string(v.Value))
						if err == nil {
							vals[i] = bv
						} else {
							vals[i] = false
							u.Errorf("could not read bool %v  %v", string(v.Value), err)
						}
					case value.NumberType:
						fv, err := strconv.ParseFloat(string(v.Value), 64)
						if err == nil {
							vals[i] = fv
						} else {
							vals[i] = float64(0)
							//u.Errorf("could not read float %v  %v", string(v.Value), err)
						}
					case value.IntType:
						iv, err := strconv.ParseInt(string(v.Value), 10, 64)
						if err == nil {
							vals[i] = iv
						} else {
							vals[i] = int64(0)
							//u.Errorf("could not read int %v  %v", string(v.Value), err)
						}
					case value.TimeType:
						dv := string(v.Value)
						// 2017-10-27 16:40:34.192944249 -0700 PDT m=-791999.995948844
						if parts := strings.Split(dv, " m="); len(parts) == 2 {
							dv = parts[0]
							if len(dv) > 20 {
								dv = dv[0 : len(dv)-4]
							}
						}
						dt, err := dateparse.ParseAny(dv)
						if err == nil {
							vals[i] = dt
						} else {
							vals[i] = time.Time{}
							u.Errorf("could not read time %v  %v", string(v.Value), err)
						}
					case value.JsonType:
						vals[i] = v.Value
					default:
						u.Warnf("wtf %+v", v)
					}

					//u.Debugf("%-2d col.name=%-10s prop.T %T\tprop.v%v", i, col.Name, v, v)
					found = true
					break
				}
			}
			if !found {
				//u.Warnf("not found? key:%v col:%v  val:%s", key, v.Column, string(v.Value))
			}
		}

		//u.Debugf("%s new row ct: %v cols:%v vals:%v", r.Key(), m.Total, colNames, vals)
		//msg := &datasource.SqlDriverMessage{vals, len(m.Vals)}
		msg := datasource.NewSqlDriverMessageMap(uint64(m.Total), vals, colNames)
		m.Total++
		//u.Debugf("In gds source iter %#v", vals)
		select {
		case <-sigChan:
			return false
		case outCh <- msg:
			// continue
		}
		//u.Debugf("vals:  %v", row.Vals)

		return true
	}, opts...)
	if err != nil {
		u.Errorf("Reading rows: %v", err)
	} else {

	}

	u.Infof("finished query, took: %v for %v rows", time.Now().Sub(queryStart), m.Total)
	return nil
}
