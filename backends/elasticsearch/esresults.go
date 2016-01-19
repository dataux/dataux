package elasticsearch

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/rel"
	"github.com/araddon/qlbridge/value"
)

var (
	// Ensure we implement Scanner
	_ rel.Task = (*ResultReader)(nil)
	//_ datasource.Scanner = (*ResultReader)(nil)
)

// Elasticsearch ResultProvider, adapts the elasticsearch http json
//   to dataux/driver values
//
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
	Req           *SqlToEs
}

// A wrapper, allowing us to implement sql/driver Next() interface
//   which is different than qlbridge/datasource Next()
type ResultReaderNext struct {
	*ResultReader
}

func NewResultReader(req *SqlToEs) *ResultReader {
	m := &ResultReader{}
	m.TaskBase = exec.NewTaskBase(req.ctx, "es-resultreader")
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
		if m.Req.hasSingleValue {
			for _, col := range sql.Columns {
				if col.CountStar() {
					cols = append(cols, rel.NewResultColumn(col.Key(), len(cols), col, value.IntType))
				} else {
					u.Debugf("why Aggs? %#v", col)
					cols = append(cols, rel.NewResultColumn(col.Key(), len(cols), col, value.IntType))
				}
			}
		} else if m.Req.hasMultiValue {
			// MultiValue returns are resultsets that have multiple rows for a single expression, ie top 10 terms for this field, etc
			// if len(sql.GroupBy) > 0 {
			// We store the Field Name Here
			//u.Debugf("why MultiValue Aggs? %#v", m.Req)
			cols = append(cols, rel.NewResultColumn("field_name", len(cols), nil, value.StringType))
			cols = append(cols, rel.NewResultColumn("key", len(cols), nil, value.StringType)) // the value of the field
			cols = append(cols, rel.NewResultColumn("count", len(cols), nil, value.IntType))
		}
	} else {
		for _, col := range m.Req.sel.Columns {
			if fld, ok := m.Req.tbl.FieldMap[col.SourceField]; ok {
				//u.Debugf("column: %#v", col)
				cols = append(cols, rel.NewResultColumn(col.SourceField, len(cols), col, fld.Type))
			} else {
				u.Debugf("Could not find: %v", col.String())
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

// Run()
//
//  Normally, finalize is responsible for ensuring schema, setu
//   but in the case of elasticsearch, since it is a non-streaming
//   response, we build out values in advance
func (m *ResultReader) Run() error {

	sigChan := m.SigChan()
	outCh := m.MessageOut()

	m.finalized = true
	m.buildProjection()

	//defer context.Recover()
	defer func() {
		close(outCh) // closing output channels is the signal to stop
		//m.TaskBase.Close()
		u.Debugf("nice, finalize ResultReader out: %p  row ct %v", outCh, len(m.Vals))
	}()

	sql := m.Req.sel

	m.Vals = make([][]driver.Value, 0)
	colNames := make(map[string]int, len(m.proj.Columns))
	for i, col := range m.proj.Columns {
		colNames[col.As] = i
	}

	if sql.Star {
		// ??
	} else if sql.CountStar() {
		// Count *
		vals := make([]driver.Value, 1)
		vals[0] = m.Total
		m.Vals = append(m.Vals, vals)

	} else if len(m.Aggs) > 0 {

		if m.Req.hasMultiValue && m.Req.hasSingleValue {
			return fmt.Errorf("Must not mix single value and multi-value aggs")
		}
		if m.Req.hasSingleValue {
			vals := make([]driver.Value, len(sql.Columns))
			for i, col := range sql.Columns {
				fldName := col.Key()
				if col.Expr != nil {
					u.Debugf("col: %v", col.Expr.String())
				}

				if col.CountStar() {
					u.Debugf("found count star")
					vals[i] = m.Total
				} else {
					u.Debugf("looking for col: %v=%v", fldName, m.Aggs.Get(fldName+"/value"))
					vals[i] = m.Aggs.Get(fldName + "/value")
				}

			}
			u.Debugf("write result: %v", vals)
			m.Vals = append(m.Vals, vals)
		} else if m.Req.hasMultiValue {
			// MultiValue returns are resultsets that have multiple rows for a single expression, ie top 10 terms for this field, etc

			if len(sql.GroupBy) > 0 {
				//for i, col := range sql.Columns {
				for i, _ := range sql.GroupBy {
					fldName := fmt.Sprintf("group_by_%d", i)
					u.Debugf("looking for col: %v  %v", fldName, m.Aggs.Get(fldName+"/results"))
					results := m.Aggs.Helpers(fldName + "/buckets")
					for _, result := range results {
						vals := make([]driver.Value, 3)
						vals[0] = fldName
						vals[1] = result.String("key")
						vals[2] = result.Int("doc_count")
						m.Vals = append(m.Vals, vals)
					}
					u.Debugf("missing value? %v", m.Aggs.Get(fldName))
					// by, _ := json.MarshalIndent(m.Aggs.Get(fldName), " ", " ")
					// vals[1] = by

				}
			} else {
				// MultiValue are generally aggregates
				for _, col := range sql.Columns {
					fldName := col.As
					u.Debugf("looking for col: %v  %v", fldName, m.Aggs.Get(fldName+"/results"))
					results := m.Aggs.Helpers(fldName + "/buckets")
					for _, result := range results {
						vals := make([]driver.Value, 3)
						vals[0] = fldName
						vals[1] = result.String("key")
						vals[2] = result.Int("doc_count")
						m.Vals = append(m.Vals, vals)
					}
				}
			}
		}

	} else {
		if err := m.pageDocs(); err != nil {
			u.Errorf("error: %v", err)
		}
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
			// continue
		}
	}

	return nil
}

func (m *ResultReader) pageDocs() error {

	metaFields := map[string]byte{"_id": 1, "_type": 1, "_score": 1}

	// If we have projected fields using  "fields=field1,field2" in Elasticsearch
	//  then it will change the response format to
	//
	//     gou.JsonHelper{"_index":"github_push", "_type":"event", "_id":"b307c13d856d95b4990f89b5df2fd667", "_score":1,
	//         "fields":map[string]interface {}{"repository.name":[]interface {}{"fluentd-ui"}, "actor":[]interface {}{"uu59"}}}
	//
	//  INSTEAD of
	//
	//     gou.JsonHelper{"_index":"github_push", "_type":"event", "_id":"b307c13d856d95b4990f89b5df2fd667", "_score":1,
	//         "_source":map[string]interface {}{"repository.name":[]interface {}{"fluentd-ui"}, "actor":[]interface {}{"uu59"}}}
	keyPath := "_source."
	useFields := false
	// if len(m.Req.projections) > 0 {
	// 	keyPath = "fields."
	// 	useFields = true
	// }

	cols := m.proj.Columns
	if len(cols) == 0 {
		u.Errorf("wat?  no cols? %v", cols)
	}
	for _, doc := range m.Docs {
		if len(doc) > 0 {
			//by, _ := json.MarshalIndent(doc, " ", " ")
			//u.Debugf("doc: %v", string(by))
			if useFields {
				doc = doc.Helper("fields")
				if len(doc) < 1 {
					u.Warnf("could not find fields? %#v", doc)
					continue
				}
			}
			vals := make([]driver.Value, len(m.proj.Columns))
			fldI := 0

			for _, col := range cols {
				key := keyPath + col.Name
				if _, ok := metaFields[col.Name]; ok {
					key = col.Name
				}
				//u.Debugf("looking for? %v in %#v", key, doc)

				if useFields {
					u.Debugf("use fields: '%s' type=%v Strings()='%v'  doc=%#v", col.Name, col.Type.String(), doc.Strings(key), doc)
					switch col.Type {
					case value.StringType:
						if docVals := doc.Strings(col.Name); len(docVals) > 0 {
							vals[fldI] = docVals[0]
						} else {
							u.Warnf("no vals for %v?  %#v", col.Name, docVals)
						}
					case value.TimeType:
						if docVals := doc.Strings(col.Name); len(docVals) > 0 {
							vals[fldI] = docVals[0]
						} else {
							u.Warnf("no vals?  %#v", docVals)
						}
					case value.IntType, value.NumberType:
						if docVals := doc.List(col.Name); len(docVals) > 0 {
							vals[fldI] = docVals[0]
						} else {
							u.Warnf("no vals?  %#v", docVals)
						}
					case value.ByteSliceType:
						u.Debugf("blob?  %v", key)
						if docVal := doc.Get(col.Name); docVal != nil {
							by, _ := json.Marshal(docVal)
							vals[fldI] = string(by)
						}
					default:
						u.Warnf("unrecognized type: %v  %T", col.Name, col.Type)
					}
				} else {

					//u.Debugf("col.type %v type %v", key, col.Type.String())

					switch col.Type {
					case value.StringType:

						strVal := doc.String(key)
						//u.Debugf("strval: %s=%q", key, strVal)
						if strVal != "" {
							vals[fldI] = strVal
						} else {
							jhVal := doc.Helper(key)
							if len(jhVal) > 0 {
								//u.Debugf("looking for? key:%v type:%s   val:%s", key, col.Type.String(), jhVal)
								jsonBytes, err := json.Marshal(jhVal)
								if err == nil {
									vals[fldI] = string(jsonBytes)
								}
							}
						}

					case value.TimeType:
						vals[fldI] = doc.String(key)
					case value.IntType:
						vals[fldI] = doc.Int64(key)
					case value.NumberType:
						vals[fldI] = doc.Float64(key)
					case value.ByteSliceType:
						//u.Debugf("blob?  %v", key)
						if docVal := doc.Get(key); docVal != nil {
							by, _ := json.Marshal(docVal)
							vals[fldI] = string(by)
						}
					default:
						u.Warnf("unrecognized type: %v  %T", col.Name, col.Type)
					}
				}

				fldI++
			}
			//u.Debugf("vals: %#v", vals)
			m.Vals = append(m.Vals, vals)
		}
	}

	return nil
}
