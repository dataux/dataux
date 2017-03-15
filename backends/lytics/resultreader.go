package lytics

import (
	"database/sql/driver"
	"encoding/json"

	"github.com/araddon/dateparse"
	u "github.com/araddon/gou"
	lytics "github.com/lytics/go-lytics"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/value"
)

var (
	// Ensure we implement Task
	_ exec.Task = (*ResultReader)(nil)
)

// ResultReader, adapts the lytics http json response
type ResultReader struct {
	*exec.TaskBase
	finalized bool
	cursor    int
	Docs      []u.JsonHelper
	Vals      [][]driver.Value
	Total     int
	Aggs      u.JsonHelper
	ScrollId  string
	Req       *Generator
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

// Run() Fetch api response
func (m *ResultReader) Run() error {

	sigChan := m.SigChan()
	outCh := m.MessageOut()
	m.finalized = true

	cols := m.Req.p.Proj.Columns
	client := lytics.NewLytics(m.Req.apiKey, "", nil)

	colNames := make(map[string]int, len(m.Req.p.Proj.Columns))
	for i, col := range m.Req.p.Proj.Columns {
		colNames[col.As] = i
	}

	defer func() {
		close(outCh) // closing output channels is the signal to stop
		u.Debugf("nice, finalize ResultReader out: %p  row ct %v", outCh, len(m.Vals))
	}()

	// create the scanner
	scan := client.PageAdHocSegment(m.Req.ql.String())

	rowCt := 0

	// handle processing the entities
	for {
		e := scan.Next()
		if e == nil {
			break
		}
		row := make([]driver.Value, len(colNames))
		eh := u.JsonHelper(e)
		for i, col := range cols {
			switch col.Type {
			case value.BoolType:
				row[i] = eh.Bool(col.As)
			case value.StringType:
				row[i] = eh.String(col.As)
			case value.TimeType:
				t, err := dateparse.ParseAny(eh.String(col.As))
				if err == nil {
					row[i] = t
				}
			case value.IntType:
				iv, ok := eh.Int64Safe(col.As)
				if ok {
					row[i] = iv
				}
			case value.NumberType:
				fv, ok := eh.Float64Safe(col.As)
				if ok {
					row[i] = fv
				}
			case value.StringsType:
				row[i] = eh.Strings(col.As)
			case value.JsonType:
				by, _ := json.Marshal(eh[col.As])
				row[i] = by
			default:
				u.Warnf("unhandled %s", col.Type)
				row[i] = eh.PrettyJson()
			}
			colNames[col.As] = i
		}

		//u.Debugf("new row ct: %v cols:%v vals:%v", len(m.Vals), colNames, vals)
		//msg := &datasource.SqlDriverMessage{vals, len(m.Vals)}
		msg := datasource.NewSqlDriverMessageMap(uint64(rowCt), row, colNames)
		//u.Infof("In source Scanner iter %#v", msg)
		select {
		case <-sigChan:
			return nil
		case outCh <- msg:
		}

		rowCt++

		//fmt.Printf("%v\n\n", e.PrettyJson())
	}

	return nil
}
