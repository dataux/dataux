// package bigquery implements a data source (backend) to allow
// dataux to query google bigquery so that bigquery power is available
// via the pervasive mysql protocol.
package bigquery

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
	u "github.com/araddon/gou"
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
	_ schema.Conn         = (*SqlToBQ)(nil)
	_ plan.SourcePlanner  = (*SqlToBQ)(nil)
	_ exec.ExecutorSource = (*SqlToBQ)(nil)
	_ schema.ConnMutation = (*SqlToBQ)(nil)

	Timeout   = 10 * time.Second
	globalCtx = context.Background()
)

// SqlToBQ Convert a Sql Query to a bigquery read/write rows
// - responsible for passing through query if possible, or
//   rewrite if necessary
type SqlToBQ struct {
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
	partition            *schema.Partition // current partition for this request
	needsPolyFill        bool              // polyfill?
	needsWherePolyFill   bool              // do we request that features be polyfilled?
	needsProjectPolyFill bool              // do we request that features be polyfilled?
	needsOrderByPolyFill bool
}

// NewSqlToBQ create a SQL ast -> BigQuery Converter
func NewSqlToBQ(s *Source, t *schema.Table) *SqlToBQ {
	m := &SqlToBQ{
		tbl:    t,
		schema: t.SchemaSource,
		s:      s,
	}
	return m
}

func (m *SqlToBQ) queryRewrite(original *rel.SqlSelect) error {

	u.Debugf("%p  %s query:%s", m, m.tbl.Name, m.sel)
	m.original = original

	req := original.Copy()
	m.sel = req
	limit := req.Limit
	if limit == 0 {
		limit = DefaultLimit
	}

	req.RewriteAsRawSelect()

	for _, from := range req.From {
		u.Infof("from: %+v", from)
		from.Name = fmt.Sprintf("[bigquery-public-data:%s.%s]", m.s.dataset, from.Name)
	}

	if req.Where != nil {
		u.Debugf("WHERE original vs new: \n%s\n%s", original.Where, req.Where)
		m.whereIdents = make(map[string]bool)
		w, err := m.walkWhereNode(req.Where.Expr)
		if err != nil {
			u.Warnf("Could Not evaluate Where Node %s %v", req.Where.Expr.String(), err)
			return err
		}
		req.Where = nil
		if w != nil {
			req.Where = rel.NewSqlWhere(w)
			u.Infof("found new Where: nil?%v  %s", req.Where == nil, req.Where)
		}
	}

	// Obviously we aren't doing group-by
	if len(req.GroupBy) > 0 {
		u.Debugf(" poly-filling groupby")
		req.GroupBy = nil
	}

	if len(req.OrderBy) > 0 {
		u.Debugf("orderby?")
		ob := req.OrderBy
		req.OrderBy = make(rel.Columns, 0, len(ob))
		for _, c := range ob {
			if c.Expr == nil {
				u.Warnf("nil expr orderby %#v", c)
			} else {
				newOrderExpr, _ := m.walkOrderBy(c.Expr)
				if newOrderExpr != nil {
					c.Expr = newOrderExpr
					u.Debugf("yay can orderby %s", c)
					req.OrderBy = append(req.OrderBy, c)
				}
			}
		}
	}

	u.Infof("%s", req)
	return nil
}

// WalkSourceSelect An interface implemented by this connection allowing the planner
// to push down as much logic into this source as possible
func (m *SqlToBQ) WalkSourceSelect(planner plan.Planner, p *plan.Source) (plan.Task, error) {
	//u.Debugf("VisitSourceSelect(): %s", p.Stmt)
	m.p = p
	return nil, nil
}

// WalkExecSource an interface of executor that allows this source to
// create its own execution Task so that it can push down as much as it can
// to bigtable.
func (m *SqlToBQ) WalkExecSource(p *plan.Source) (exec.Task, error) {

	//u.Debugf("%p WalkExecSource():  %T nil?%v %#v", m, p, p == nil, p)
	//u.Debugf("%p WalkExecSource()  %s", m, p.Stmt)

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
					}
				}
			}
		}
	}

	err := m.queryRewrite(p.Stmt.Source)
	if err != nil {
		return nil, nil
	}

	reader := NewResultReader(m)
	m.resp = reader

	// For aggregations, group-by, or limit clauses we will need to do final
	// aggregation here in master as the reduce step
	if m.sel.IsAggQuery() {
		u.Debugf("Adding aggregate/group by?")
		gbplan := plan.NewGroupBy(m.sel)
		gb := exec.NewGroupByFinal(m.Ctx, gbplan)
		reader.Add(gb)
		m.needsPolyFill = true
	}

	if m.needsWherePolyFill {
		wp := plan.NewWhere(m.sel)
		wt := exec.NewWhere(m.Ctx, wp)
		reader.Add(wt)
		m.needsPolyFill = true
	}

	// do we need poly fill Having?
	if m.sel.Having != nil {
		u.Infof("needs HAVING polyfill")
	}

	if m.needsOrderByPolyFill {
		u.Infof("adding order by poly fill")
		op := plan.NewOrder(m.sel)
		ot := exec.NewOrder(m.Ctx, op)
		reader.Add(ot)
		m.needsPolyFill = true
	}

	u.Infof("%p  needsPolyFill?%v  limit:%d ", m.sel, m.needsPolyFill, m.sel.Limit)
	if m.needsPolyFill {
		if m.sel.Limit > 0 {
			// Since we are poly-filling we need to over-read
			// because group-by's, order-by's, where poly-fills mean
			// cass limits aren't valid from original statement
			m.sel.Limit = 0
			reader.Req.sel.Limit = 0
			u.Warnf("%p setting limit up!!!!!! %v", m.sel, m.sel.Limit)
		}
	}

	return reader, nil
}

// CreateMutator part of Mutator interface to allow data sources create a stateful
//  mutation context for update/delete operations.
func (m *SqlToBQ) CreateMutator(pc interface{}) (schema.ConnMutator, error) {
	if ctx, ok := pc.(*plan.Context); ok && ctx != nil {
		m.TaskBase = exec.NewTaskBase(ctx)
		m.stmt = ctx.Stmt
		return m, nil
	}
	return nil, fmt.Errorf("Expected *plan.Context but got %T", pc)
}

// Put Interface for mutation (insert, update)
func (m *SqlToBQ) Put(ctx context.Context, key schema.Key, val interface{}) (schema.Key, error) {

	if m.schema == nil {
		u.Warnf("must have schema")
		return nil, fmt.Errorf("Must have schema for updates in bigtable")
	}

	if m.tbl.Parent == "" {
		return nil, fmt.Errorf("Must have parent for big-table put")
	}

	if key == nil {
		u.Warnf("didn't have key?  %v", val)
		// If we don't have a key we MUST choose one from columns via
		// the schema ie the "primary key"
		//return nil, fmt.Errorf("Must have key for updates in bigtable")
	}

	cols := m.tbl.Columns()
	if m.stmt == nil {
		return nil, fmt.Errorf("Must have stmts to infer columns ")
	}

	switch q := m.stmt.(type) {
	case *rel.SqlInsert:
		cols = q.ColumnNames()
	default:
		return nil, fmt.Errorf("%T not yet supported ", q)
	}

	row := newRowVals()
	colNames := make(map[string]int, len(cols))

	for i, colName := range cols {
		colNames[colName] = i
	}

	switch valT := val.(type) {
	case []driver.Value:

		//u.Debugf("row:  %v", valT)
		//u.Debugf("row len=%v   fieldlen=%v col len=%v", len(valT), len(m.tbl.Fields), len(cols))
		for _, f := range m.tbl.Fields {
			for i, colName := range cols {
				if f.Name == colName {
					if len(valT) <= i-1 {
						u.Errorf("bad column count?  %d vs %d  col: %+v", len(valT), i, f)
					} else {
						switch val := valT[i].(type) {
						case string, []byte, int, int64, bool, time.Time:
							row.vals[f.Name] = val
						case []value.Value:
							switch f.Type {
							case value.StringsType:
								vals := make([]string, len(val))
								for si, sv := range val {
									vals[si] = sv.ToString()
								}

								row.vals[f.Name] = vals

							default:
								u.Warnf("what type? %s", f.Type)
								by, err := json.Marshal(val)
								if err != nil {
									u.Errorf("Error converting field %v  err=%v", val, err)
									row.vals[f.Name] = ""
								} else {
									row.vals[f.Name] = string(by)
								}
								u.Debugf("PUT field: i=%d col=%s row[i]=%v  T:%T", i, colName, string(by), by)
							}

						default:
							u.Warnf("unsupported conversion: %T  %v", val, val)
						}
					}
					break
				}
			}
		}

	case map[string]driver.Value:
		for _, f := range m.tbl.Fields {
			for colName, driverVal := range valT {
				if f.Name == colName {
					switch val := driverVal.(type) {
					case string, []byte, int, int64, bool:
						row.vals[f.Name] = val
					case time.Time:
						row.vals[f.Name] = val
					case []value.Value:
						by, err := json.Marshal(val)
						if err != nil {
							u.Errorf("Error converting field %v  err=%v", val, err)
						}
						row.vals[f.Name] = by
					default:
						u.Warnf("unsupported conversion: %T  %v", val, val)

					}
					break
				}
			}
		}

	default:
		u.Warnf("unsupported type: %T  %#v", val, val)
		return nil, fmt.Errorf("Was not []driver.Value?  %T", val)
	}

	goctx := context.Background()
	// [START bigquery_insert_stream]
	client, err := bigquery.NewClient(context.Background(), m.s.dataProject)
	if err != nil {
		u.Warnf("Could not create bigquery client %v", err)
		return nil, err
	}

	tu := client.Dataset(m.s.dataset).Table(m.tbl.Name).Uploader()
	if err := tu.Put(goctx, row); err != nil {
		return nil, err
	}

	newKey := datasource.NewKeyCol("id", "fixme")
	return newKey, nil
}

type RowVals struct {
	vals map[string]bigquery.Value
	id   string
}

func newRowVals() *RowVals {
	return &RowVals{vals: make(map[string]bigquery.Value)}
}

// Save implements the ValueSaver interface.
func (r *RowVals) Save() (map[string]bigquery.Value, string, error) {
	return r.vals, r.id, nil
}

func (m *SqlToBQ) PutMulti(ctx context.Context, keys []schema.Key, src interface{}) ([]schema.Key, error) {
	return nil, schema.ErrNotImplemented
}

// Delete delete by row key
func (m *SqlToBQ) Delete(key driver.Value) (int, error) {
	u.Warnf("not implemented delete?  %v", key)
	return 0, schema.ErrNotImplemented
}

// DeleteExpression - delete by expression (where clause)
//  - For where columns contained in Partition Keys we can push to bigtable
//  - for others we might have to do a select -> delete
func (m *SqlToBQ) DeleteExpression(p interface{}, where expr.Node) (int, error) {
	return 0, schema.ErrNotImplemented
}

// walkWhereNode() We are re-writing the sql select statement and need
// to walk the ast and see if we can partially/completely translate
// the logical filtesr into big-table filters or prefix scans
func (m *SqlToBQ) walkWhereNode(cur expr.Node) (expr.Node, error) {
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

func (m *SqlToBQ) walkArg(cur expr.Node) (expr.Node, error) {
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

func (m *SqlToBQ) walkFilterBinary(node *expr.BinaryNode) (expr.Node, error) {

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

	u.Debugf("\nlh: %#v\nrh: %#v", node.Args[0], node.Args[1])
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
		//
	case lex.TokenLike:
		// hmmmmmmm
		// what about byte prefix ?
		return nil, nil
	default:
		u.Warnf("not implemented: %s", node)
	}
	return nil, nil
}

func (m *SqlToBQ) walkOrderBy(node expr.Node) (expr.Node, error) {
	switch n := node.(type) {
	case *expr.IdentityNode:
		if m.canOrder(n) {
			return node, nil
		}
	default:
		m.needsOrderByPolyFill = true
		u.Warnf("un-handled order clause expression type?  %T %#v", node, n)
		//return fmt.Errorf("Not implemented node: %s", n)
	}
	return nil, nil
}

func (m *SqlToBQ) canOrder(in *expr.IdentityNode) bool {

	_, lhIdentityName, _ := in.LeftRight()
	_, exists := m.tbl.FieldMap[lhIdentityName]
	if !exists {
		// Doesn't exist in cassandra? possibly json path?
		return false
	}

	return true
}
