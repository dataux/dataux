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

// ResultReaderNext is wrapper, allowing us to implement sql/driver Next() interface
// which is different than qlbridge/datasource Next()
type ResultReaderNext struct {
	*ResultReader
}

// NewResultReader create a new lytics reader that will handle
// translation of lytics rest api content into dataux/qlbridge types.
func NewResultReader(req *Generator) *ResultReader {
	m := &ResultReader{}
	m.TaskBase = exec.NewTaskBase(req.p.Context())
	m.Req = req
	return m
}

func (m *ResultReader) Close() error { return nil }

// Run() Fetch api response, page through result continuing to
// send messages.
func (m *ResultReader) Run() error {

	sigChan := m.SigChan()
	outCh := m.MessageOut()
	m.finalized = true

	// projection Columns
	cols := m.Req.p.Proj.Columns
	client := lytics.NewLytics(m.Req.apiKey, "", nil)

	colNames := make(map[string]int, len(m.Req.p.Proj.Columns))
	for i, col := range m.Req.p.Proj.Columns {
		colNames[col.SourceName()] = i
	}

	defer func() {
		close(outCh) // closing output channels is the signal to stop
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
				row[i] = eh.Bool(col.SourceName())
			case value.StringType:
				row[i] = eh.String(col.SourceName())
			case value.TimeType:
				t, err := dateparse.ParseAny(eh.String(col.SourceName()))
				if err == nil {
					row[i] = t
				}
			case value.IntType:
				iv, ok := eh.Int64Safe(col.SourceName())
				if ok {
					row[i] = iv
				}
			case value.NumberType:
				fv, ok := eh.Float64Safe(col.SourceName())
				if ok {
					row[i] = fv
				}
			case value.StringsType:
				row[i] = eh.Strings(col.SourceName())
			case value.JsonType:
				by, _ := json.Marshal(eh[col.SourceName()])
				row[i] = by
			default:
				u.Warnf("unhandled %s", col.Type)
				row[i] = eh.PrettyJson()
			}
			//u.Debugf("%q  %T  %v", col.As, row[i], row[i])
		}

		msg := datasource.NewSqlDriverMessageMap(uint64(rowCt), row, colNames)
		select {
		case <-sigChan:
			return nil
		case outCh <- msg:
		}

		rowCt++
	}

	return nil
}
