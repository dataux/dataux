package lytics

import (
	"fmt"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/plan"
	"github.com/araddon/qlbridge/rel"
	"github.com/araddon/qlbridge/schema"
	"github.com/araddon/qlbridge/value"
)

var (
	DefaultLimit = 1000

	// Implement Push Down interface
	_ plan.SourcePlanner  = (*Generator)(nil)
	_ exec.ExecutorSource = (*Generator)(nil)
)

type esMap map[string]interface{}

// Sql To Lytics Request Generator
//   Map sql queries into Lytics Api Requests
type Generator struct {
	resp          *ResultReader
	p             *plan.Source
	tbl           *schema.Table
	sel           *rel.SqlSelect
	schema        *schema.SchemaSource
	ctx           *plan.Context
	partition     *schema.Partition // current partition for this request
	needsPolyFill bool              // do we request that features be polyfilled?
}

func NewGenerator(table *schema.Table) *Generator {
	return &Generator{
		tbl:    table,
		schema: table.SchemaSource,
	}
}

func (m *Generator) Close() error { return nil }

func (m *Generator) Columns() []string {
	return m.resp.Columns()
}

// WalkSourceSelect is used during planning phase, to create a plan (plan.Task)
//  or error, and to report back any poly-fill necessary
func (m *Generator) WalkSourceSelect(planner plan.Planner, p *plan.Source) (plan.Task, error) {
	u.Warnf("not implemented")
	return nil, nil
}

func (m *Generator) WalkExecSource(p *plan.Source) (exec.Task, error) {

	u.Warnf("not implemented")
	if p.Stmt == nil {
		return nil, fmt.Errorf("Plan did not include Sql Statement?")
	}
	if p.Stmt.Source == nil {
		return nil, fmt.Errorf("Plan did not include Sql Select Statement?")
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
					}
				}
			}
		}
	}

	m.sel = p.Stmt.Source

	if m.sel.Where != nil {
		ql := &rel.FilterStatement{}
		ql.Filter = m.sel.Where.Expr
		ql.From = m.sel.From[0].Name
		u.Infof("query %v", ql)
	} else {
		u.Warnf("NO WHERE/FILTER? %v", m.sel)
	}

	//req := p.Stmt.Source
	reader := NewResultReader(m)
	p.Complete = false
	return reader, nil
}

// Aggregations from the <select_list>
//
//    SELECT <select_list> FROM ... WHERE
//
func (m *Generator) WalkSelectList() error {
	return nil
}

// Aggregations from the <select_list>
//
//    WHERE .. GROUP BY x,y,z
//
func (m *Generator) WalkGroupBy() error {
	return nil
}

// WalkAggs() aggregate expressions when used ast part of <select_list>
//  - For Aggregates (functions) it builds aggs
//  - For Projectsion (non-functions) it does nothing, that will be done later during projection
func (m *Generator) WalkAggs(cur expr.Node) (q esMap, _ error) {
	return q, nil
}

// Walk() an expression
func (m *Generator) WalkNode(cur expr.Node, q *esMap) (value.Value, error) {
	return nil, nil
}

func (m *Generator) walkFilterTri(node *expr.TriNode, q *esMap) (value.Value, error) {
	return nil, fmt.Errorf("not implemented")
}

func (m *Generator) walkFilterBinary(node *expr.BinaryNode, q *esMap) (value.Value, error) {
	return nil, fmt.Errorf("not implemented %v", node.String())
}

// Take an expression func, ensure we don't do runtime-checking (as the function)
//   doesn't really exist, then map that function to an ES Filter
//
//    exists, missing, prefix, term
//
func (m *Generator) walkFilterFunc(node *expr.FuncNode, q *esMap) (value.Value, error) {
	return nil, fmt.Errorf("not implemented %v", node.String())
}

func (m *Generator) walkAggFunc(node *expr.FuncNode) (q esMap, _ error) {
	return nil, fmt.Errorf("not implemented")
}

func eval(cur expr.Node) (value.Value, bool) {
	switch curNode := cur.(type) {
	case *expr.IdentityNode:
		return value.NewStringValue(curNode.Text), true
	case *expr.StringNode:
		return value.NewStringValue(curNode.Text), true
	default:
		u.Errorf("unrecognized T:%T  %v", cur, cur)
	}
	return value.NilValueVal, false
}
