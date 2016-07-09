// Cassandra implements a data source (backend) to allow
// dataux to query cassandra via sql
package cassandra

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"time"

	u "github.com/araddon/gou"
	"github.com/gocql/gocql"
	"golang.org/x/net/context"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/plan"
	"github.com/araddon/qlbridge/rel"
	"github.com/araddon/qlbridge/schema"
	"github.com/araddon/qlbridge/value"
)

var (
	// Default page limit
	DefaultLimit = 5000

	// Ensure we implment appropriate interfaces
	_ schema.Conn         = (*SqlToCql)(nil)
	_ plan.SourcePlanner  = (*SqlToCql)(nil)
	_ exec.ExecutorSource = (*SqlToCql)(nil)
	_ schema.ConnMutation = (*SqlToCql)(nil)
)

// SqlToCql Convert a Sql Query to a cassandra CQL query
// - responsible for pushing down as much logic to Cql as possible
// - dialect translator
type SqlToCql struct {
	*exec.TaskBase
	resp          *ResultReader
	tbl           *schema.Table
	p             *plan.Source
	sel           *rel.SqlSelect
	stmt          rel.SqlStatement
	schema        *schema.SchemaSource
	s             *Source
	q             *gocql.Query
	partition     *schema.Partition // current partition for this request
	needsPolyFill bool              // do we request that features be polyfilled?
}

func NewSqlToCql(s *Source, t *schema.Table) *SqlToCql {
	m := &SqlToCql{
		tbl:    t,
		schema: t.SchemaSource,
		s:      s,
	}
	u.Infof("create sqltodatasource %p", m)
	return m
}

func (m *SqlToCql) Query(req *rel.SqlSelect) (*ResultReader, error) {

	u.Debugf("%p  %s query:%s", m, m.tbl.Name, m.sel)
	m.sel = req
	limit := req.Limit
	if limit == 0 {
		limit = DefaultLimit
	}

	resultReader := NewResultReader(m)
	m.resp = resultReader
	//resultReader.Finalize()
	return resultReader, nil
}

func (m *SqlToCql) WalkSourceSelect(planner plan.Planner, p *plan.Source) (plan.Task, error) {
	u.Debugf("VisitSourceSelect(): %s", p.Stmt)
	m.p = p
	return nil, nil
}

func (m *SqlToCql) WalkExecSource(p *plan.Source) (exec.Task, error) {

	if p.Stmt == nil {
		return nil, fmt.Errorf("Plan did not include Sql Statement?")
	}
	if p.Stmt.Source == nil {
		return nil, fmt.Errorf("Plan did not include Sql Select Statement?")
	}
	if m.TaskBase == nil {
		m.TaskBase = exec.NewTaskBase(p.Context())
	}
	if m.p == nil {
		u.Debugf("custom? %v", p.Custom)

		m.p = p
		if p.Custom.Bool("poly_fill") {
			m.needsPolyFill = true
		}
		if partitionId := p.Custom.String("partition"); partitionId != "" {
			if p.Tbl.Partition != nil {
				for _, pt := range p.Tbl.Partition.Partitions {
					if pt.Id == partitionId {
						//u.Debugf("partition: %s   %#v", partitionId, pt)
						m.partition = pt
						if pt.Left == "" {
							//m.filter = bson.M{p.Tbl.Partition.Keys[0]: bson.M{"$lt": pt.Right}}
						} else if pt.Right == "" {
							//m.filter = bson.M{p.Tbl.Partition.Keys[0]: bson.M{"$gte": pt.Left}}
						} else {

						}
					}
				}
			}
		}
	}

	u.Debugf("WalkExecSource():  %T  %#v", p, p)
	u.Debugf("%p walkexec: %#v", m, m.TaskBase)
	m.Ctx = p.Context()
	m.TaskBase = exec.NewTaskBase(m.Ctx)
	reader, err := m.Query(p.Stmt.Source)
	if err != nil {
		return nil, nil
	}
	return reader, nil
}

// CreateMutator part of Mutator interface to allow data sources create a stateful
//  mutation context for update/delete operations.
func (m *SqlToCql) CreateMutator(pc interface{}) (schema.ConnMutator, error) {
	if ctx, ok := pc.(*plan.Context); ok && ctx != nil {
		m.TaskBase = exec.NewTaskBase(ctx)
		m.stmt = ctx.Stmt
		return m, nil
	}
	return nil, fmt.Errorf("Expected *plan.Context but got %T", pc)
}

// Put Interface for mutation (insert, update)
func (m *SqlToCql) Put(ctx context.Context, key schema.Key, val interface{}) (schema.Key, error) {

	if key == nil {
		u.Warnf("didn't have key?  %v", val)
		//return nil, fmt.Errorf("Must have key for updates in cassandra")
	}

	if m.schema == nil {
		u.Warnf("must have schema")
		return nil, fmt.Errorf("Must have schema for updates in cassandra")
	}

	cols := m.tbl.Columns()
	var row []driver.Value
	colNames := make(map[string]int, len(cols))

	upsertCql := "TODO:  rewrite insert/update"

	for i, colName := range cols {
		colNames[colName] = i
	}
	curRow := make([]interface{}, len(cols))

	switch valT := val.(type) {
	case []driver.Value:
		row = valT
		//u.Infof("row len=%v   fieldlen=%v col len=%v", len(row), len(m.tbl.Fields), len(cols))
		for _, f := range m.tbl.Fields {
			for i, colName := range cols {
				if f.Name == colName {
					if len(row) <= i-1 {
						u.Errorf("bad column count?  %d vs %d  col: %+v", len(row), i, f)
					} else {
						//u.Debugf("col info?  %d vs %d  col: %+v", len(row), i, f)
						switch val := row[i].(type) {
						case string, []byte, int, int64, bool, time.Time:
							u.Debugf("PUT field: i=%d col=%s row[i]=%v  T:%T", i, colName, row[i], row[i])
							//props = append(props, datastore.Property{Name: f.Name, Value: val})
							curRow[i] = val
						case []value.Value:
							by, err := json.Marshal(val)
							if err != nil {
								u.Errorf("Error converting field %v  err=%v", val, err)
							}
							u.Debugf("PUT field: i=%d col=%s row[i]=%v  T:%T", i, colName, string(by), by)
							//props = append(props, datastore.Property{Name: f.Name, Value: by})
						default:
							u.Warnf("unsupported conversion: %T  %v", val, val)
							//props = append(props, datastore.Property{Name: f.Name, Value: val})
						}
					}
					break
				}
			}
		}

	case map[string]driver.Value:
		for i, f := range m.tbl.Fields {
			for colName, driverVal := range valT {
				if f.Name == colName {
					//u.Debugf("PUT field: i=%d col=%s val=%v  T:%T cur:%v", i, colName, driverVal, driverVal, curRow[i])
					switch val := driverVal.(type) {
					case string, []byte, int, int64, bool:
						curRow[i] = val
					case time.Time:
						curRow[i] = val
					case []value.Value:
						by, err := json.Marshal(val)
						if err != nil {
							u.Errorf("Error converting field %v  err=%v", val, err)
						}
						curRow[i] = by
					default:
						u.Warnf("unsupported conversion: %T  %v", val, val)

					}
					break
				}
			}
			//u.Infof(" %v curRow? %d %#v", f.Name, len(curRow), curRow)
			//u.Debugf("%d writing %-10s %T\t%v", i, f.Name, curRow[i], curRow[i])
			//props = append(props, datastore.Property{Name: f.Name, Value: curRow[i]})
		}

	default:
		u.Warnf("unsupported type: %T  %#v", val, val)
		return nil, fmt.Errorf("Was not []driver.Value?  %T", val)
	}

	err := m.s.session.Query(upsertCql, curRow...).Exec()
	u.Infof("err %v", err)
	newKey := datasource.NewKeyCol("id", "fixme")
	return newKey, nil
}

func (m *SqlToCql) PutMulti(ctx context.Context, keys []schema.Key, src interface{}) ([]schema.Key, error) {
	return nil, schema.ErrNotImplemented
}

func (m *SqlToCql) Delete(key driver.Value) (int, error) {
	return 0, schema.ErrNotImplemented
}
func (m *SqlToCql) DeleteExpression(where expr.Node) (int, error) {
	delKey := datasource.KeyFromWhere(where)
	if delKey != nil {
		return m.Delete(delKey.Key())
	}
	return 0, fmt.Errorf("Could not delete with that where expression: %s", where)
}
