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
	"github.com/araddon/qlbridge/lex"
	"github.com/araddon/qlbridge/plan"
	"github.com/araddon/qlbridge/rel"
	"github.com/araddon/qlbridge/schema"
	"github.com/araddon/qlbridge/value"
	"github.com/araddon/qlbridge/vm"
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
	resp                 *ResultReader
	tbl                  *schema.Table
	p                    *plan.Source
	sel                  *rel.SqlSelect
	original             *rel.SqlSelect
	whereIdents          map[string]bool
	stmt                 rel.SqlStatement
	schema               *schema.SchemaSource
	s                    *Source
	q                    *gocql.Query
	cf                   *gocql.TableMetadata
	partition            *schema.Partition // current partition for this request
	needsPolyFill        bool              // polyfill?
	needsWherePolyFill   bool              // do we request that features be polyfilled?
	needsProjectPolyFill bool              // do we request that features be polyfilled?
}

// NewSqlToCql create a SQL -> CQL ast converter
func NewSqlToCql(s *Source, t *schema.Table) *SqlToCql {
	m := &SqlToCql{
		tbl:    t,
		schema: t.SchemaSource,
		s:      s,
	}
	return m
}

func (m *SqlToCql) query(original *rel.SqlSelect) (*ResultReader, error) {

	//u.Debugf("%p  %s query:%s", m, m.tbl.Name, m.sel)
	m.original = original

	cf, ok := m.tbl.Context["cass_table"].(*gocql.TableMetadata)
	if !ok {
		return nil, fmt.Errorf("What, expected gocql.TableMetadata but got %T", m.tbl.Context["cass_table"])
	}
	m.cf = cf

	req := original.Copy()
	m.sel = req
	limit := req.Limit
	if limit == 0 {
		limit = DefaultLimit
	}

	if req.Where != nil {
		u.Debugf("original vs new: \n%s\n%s", original.Where, req.Where)
		m.whereIdents = make(map[string]bool)
		w, err := m.walkWhereNode(req.Where.Expr)
		if err != nil {
			u.Warnf("Could Not evaluate Where Node %s %v", req.Where.Expr.String(), err)
			return nil, err
		}
		req.Where = nil
		if w != nil {
			req.Where = rel.NewSqlWhere(w)
			u.Infof("found new Where: nil?%v  %s", req.Where == nil, req.Where)
		}
	}

	// Obviously we aren't doing group-by
	if req.GroupBy != nil {
		u.Debugf(" poly-filling groupby")
		req.GroupBy = nil
	}

	u.Infof("%s", req)

	resultReader := NewResultReader(m, req.String())
	m.resp = resultReader
	return resultReader, nil
}

// WalkSourceSelect An interface implemented by this connection allowing the planner
// to push down as much logic into this source as possible
func (m *SqlToCql) WalkSourceSelect(planner plan.Planner, p *plan.Source) (plan.Task, error) {
	//u.Debugf("VisitSourceSelect(): %s", p.Stmt)
	m.p = p
	return nil, nil
}

// WalkExecSource an interface of executor that allows this source to
// create its own execution Task so that it can push down as much as it can
// to cassandra.
func (m *SqlToCql) WalkExecSource(p *plan.Source) (exec.Task, error) {

	//u.Debugf("%p WalkExecSource():  %T nil?%v %#v", m, p, p == nil, p)
	//u.Debugf("m? %v", m == nil)

	if p.Stmt == nil {
		return nil, fmt.Errorf("Plan did not include Sql Statement?")
	}
	if p.Stmt.Source == nil {
		return nil, fmt.Errorf("Plan did not include Sql Select Statement?")
	}

	if m.TaskBase == nil {
		m.TaskBase = exec.NewTaskBase(p.Context())
	}
	m.Ctx = p.Context()
	//m.TaskBase = exec.NewTaskBase(m.Ctx)

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

	reader, err := m.query(p.Stmt.Source)
	if err != nil {
		return nil, nil
	}

	// For aggregations, group-by, or limit clauses we will need to do final
	// aggregation here in master as the reduce step
	if m.sel.IsAggQuery() {
		u.Debugf("Adding aggregate/group by?")
		gbplan := plan.NewGroupBy(m.sel)
		gb := exec.NewGroupByFinal(m.Ctx, gbplan)
		reader.Add(gb)
	}

	if m.needsWherePolyFill {
		wp := plan.NewWhere(m.sel)
		wt := exec.NewWhere(m.Ctx, wp)
		reader.Add(wt)
	}
	// do we need poly fill Having?
	if m.sel.Having != nil {
		u.Infof("needs HAVING polyfill")
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

	// TODO:
	// 1) rewrite statement
	// 2) move to ?? some place more like a prepared statement

	cols := m.tbl.Columns()
	if m.stmt == nil {
		return nil, fmt.Errorf("Must have stmts to infer columns ")
	}
	switch q := m.stmt.(type) {
	case *rel.SqlInsert:
		cols = q.ColumnNames()
		u.Debugf("nice, columns %v", cols)
	}

	var row []driver.Value
	colNames := make(map[string]int, len(cols))

	// TODO:  rewrite insert/update"
	upsertCql := m.stmt.String()

	for i, colName := range cols {
		colNames[colName] = i
	}
	curRow := make([]interface{}, len(cols))

	switch valT := val.(type) {
	case []driver.Value:
		row = valT
		u.Infof("row len=%v   fieldlen=%v col len=%v", len(row), len(m.tbl.Fields), len(cols))
		for _, f := range m.tbl.Fields {
			for i, colName := range cols {
				if f.Name == colName {
					if len(row) <= i-1 {
						u.Errorf("bad column count?  %d vs %d  col: %+v", len(row), i, f)
					} else {
						u.Debugf("col info?  %d vs %d  col: %+v", len(row), i, f)
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

	u.Debugf("%s", upsertCql)
	err := m.s.session.Query(upsertCql, curRow...).Exec()
	if err != nil {
		u.Errorf("could not insert: %v", err)
		return nil, err
	}
	u.Infof("err %v", err)
	newKey := datasource.NewKeyCol("id", "fixme")
	return newKey, nil
}

func (m *SqlToCql) PutMulti(ctx context.Context, keys []schema.Key, src interface{}) ([]schema.Key, error) {
	return nil, schema.ErrNotImplemented
}

// Delete delete by row
func (m *SqlToCql) Delete(key driver.Value) (int, error) {
	return 0, schema.ErrNotImplemented
}

// DeleteExpression - delete by expression (where clause)
//  - For where columns contained in Partition Keys we can push to cassandra
//  - for others we might have to do a select -> delete
func (m *SqlToCql) DeleteExpression(where expr.Node) (int, error) {
	return 0, schema.ErrNotImplemented
}

func (m *SqlToCql) isCassKey(name string) bool {
	for _, cc := range m.cf.PartitionKey {
		if cc.Name == name {
			return true
		}
	}
	for _, cc := range m.cf.ClusteringColumns {
		if cc.Name == name {
			return true
		}
	}
	return false
}

// walkWhereNode() We are re-writing the sql select statement and need
//   to walk the ast and see if we can push down this where clause
//   completely or partially.
//
//  Limititations of Where in Cassandra
//  - no functions/expressions straight simple operators
//  - operators  [=, >=, <=, !=, IN ]
//  - MUST follow rules of partition keys ie all partition keys to the "left"
//    of each filter field must also be in filter
//
func (m *SqlToCql) walkWhereNode(cur expr.Node) (expr.Node, error) {
	//u.Debugf("walkWhereNode: %s", cur)
	switch n := cur.(type) {
	case *expr.BinaryNode:
		return m.walkFilterBinary(n)
	case *expr.TriNode: // Between
		m.needsWherePolyFill = true
		//return fmt.Errorf("Between and other ops not implemented")
		u.Warnf("between being polyfilled %s", n)
	case *expr.UnaryNode:
		m.needsWherePolyFill = true
		u.Warnf("UnaryNode being polyfilled %s", n)
		// Check to see if this is a boolean field?
	case *expr.FuncNode:
		m.needsWherePolyFill = true
		if len(n.Args) > 0 {
			idents := expr.FindAllIdentityField(n)
			u.Infof("Found identities in func():    %s  idents:%v", n, idents)
			// What can we do with this knowledge? no much right?
		}
		//return fmt.Errorf("Not implemented function: %s", n)
	default:
		m.needsWherePolyFill = true
		u.Warnf("un-handled where clause expression type?  %T %#v", cur, n)
		//return fmt.Errorf("Not implemented node: %s", n)
	}
	return nil, nil
}

func (m *SqlToCql) walkArg(cur expr.Node) (expr.Node, error) {
	//u.Debugf("walkArg: %s", cur)
	switch n := cur.(type) {
	case *expr.BinaryNode:
		return m.walkFilterBinary(n)
	case *expr.TriNode: // Between
		m.needsWherePolyFill = true
		//return fmt.Errorf("Between and other ops not implemented")
		u.Warnf("between being polyfilled %s", n)
	case *expr.UnaryNode:
		m.needsWherePolyFill = true
		u.Warnf("UnaryNode being polyfilled %s", n)
		// Check to see if this is a boolean field?
	case *expr.FuncNode:
		m.needsWherePolyFill = true
		if len(n.Args) > 0 {
			idents := expr.FindAllIdentityField(n)
			u.Infof("Found identities in func():    %s  idents:%v", n, idents)
			// What can we do with this knowledge? no much right?
			// possibly ensure not nil?
		}
		//return fmt.Errorf("Not implemented function: %s", n)
	default:
		m.needsWherePolyFill = true
		u.Warnf("un-handled where clause expression type?  %T %#v", cur, n)
		//return fmt.Errorf("Not implemented node: %s", n)
	}
	return nil, nil
}

func (m *SqlToCql) walkFilterBinary(node *expr.BinaryNode) (expr.Node, error) {

	in, isIdent := node.Args[0].(*expr.IdentityNode)
	if !isIdent {
		u.Warnf("Not identity node on left? %T", node.Args[0])
		return nil, nil
	}

	_, lhIdentityName, _ := in.LeftRight()
	col, exists := m.tbl.FieldMap[lhIdentityName]
	if !exists {
		// Doesn't exist in cassandra?
		// 1) child element of a row, ie json key inside column?  need to polyfill
		return nil, nil
	}
	// if it is an identity lets make sure it is in parition or cluster key
	if !m.isCassKey(lhIdentityName) {
		m.needsPolyFill = true
		u.Warnf("cannot use [%s] in WHERE due to not being part of key", node)
		return nil, nil
	}

	lhval, lhok := vm.Eval(nil, node.Args[0])
	rhval, rhok := vm.Eval(nil, node.Args[1])
	if !lhok || !rhok {
		u.Warnf("not ok: %v  l:%v  r:%v", node, lhval, rhval)
		return nil, fmt.Errorf("could not evaluate: %v", node.String())
	}
	//u.Debugf("walkBinary: %v  l:%v  r:%v  %T  %T", node, lhval, rhval, lhval, rhval)
	switch node.Operator.T {
	case lex.TokenLogicAnd:
		newArgs := make([]expr.Node, 0, 2)
		for _, arg := range node.Args {
			nn, err := m.walkWhereNode(arg)
			if err != nil {
				u.Errorf("could not evaluate where nodes? %v %s", err, arg)
				return nil, fmt.Errorf("could not evaluate: %s", arg.String())
			}
			if nn != nil {
				newArgs = append(newArgs, nn)
			}
		}
		if len(newArgs) == 0 {
			return nil, nil
		} else if len(newArgs) == 1 {
			return newArgs[0], nil
		}
		// else the original expression is valid
		return node, nil
	case lex.TokenLogicOr:
		// ??
	case lex.TokenEqual, lex.TokenEqualEqual, lex.TokenNE:
		return node, nil
	case lex.TokenLE, lex.TokenLT, lex.TokenGE, lex.TokenGT:
		if !col.Type.IsNumeric() {
			return nil, fmt.Errorf("%s Operator can only act on Numeric Column: [%s]", node.Operator.T, node)
		}
		return node, nil
	case lex.TokenIN:
		// https://lostechies.com/ryansvihla/2014/09/22/cassandra-query-patterns-not-using-the-in-query-for-multiple-partitions/
	case lex.TokenLike:
		// hmmmmmmm
		// what about byte prefix ?
		return nil, nil
	default:
		u.Warnf("not implemented: %s", node)
	}
	return nil, nil
}
