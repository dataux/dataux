package lytics

import (
	"fmt"

	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/plan"
	"github.com/araddon/qlbridge/rel"
	"github.com/araddon/qlbridge/schema"
)

var (
	// DefaultLimit is default limit page size.
	DefaultLimit = 1000

	// Implement Push Down Planner interface, this allows
	// the Generator do do its own planning, taking over from
	// default sql planner as we are going to push down as
	// much as possible to lytics api
	_ plan.SourcePlanner  = (*Generator)(nil)
	_ exec.ExecutorSource = (*Generator)(nil)

	// Prebuild a statement for no where clause
	filterAll expr.Node
)

func init() {
	fs := rel.MustParseFilter("FILTER *")
	filterAll = fs.Filter
}

// Generator Generate Lytics api requests from Sql Statements
// - convert sql WHERE into SegmentQL api statements
// - fetch entities from entity scan api
type Generator struct {
	resp          *ResultReader
	p             *plan.Source
	ql            *rel.FilterStatement
	tbl           *schema.Table
	sel           *rel.SqlSelect
	schema        *schema.Schema
	partition     *schema.Partition // current partition for this request
	needsPolyFill bool              // do we request that features be polyfilled?
	apiKey        string
}

// NewGenerator create a new generator to generate lytics query from request.
func NewGenerator(table *schema.Table, apiKey string) *Generator {
	return &Generator{
		tbl:    table,
		schema: table.Schema,
		apiKey: apiKey,
	}
}

// Close this generator
func (m *Generator) Close() error { return nil }

// WalkSourceSelect is used during planning phase, to create a plan (plan.Task)
// or error, and to report back any poly-fill necessary
func (m *Generator) WalkSourceSelect(planner plan.Planner, p *plan.Source) (plan.Task, error) {
	m.p = p
	return nil, nil
}

// WalkExecSource allow this to do its own Exec planning.
func (m *Generator) WalkExecSource(p *plan.Source) (exec.Task, error) {

	if p.Context() == nil {
		return nil, fmt.Errorf("Missing plan context")
	}

	if p.Stmt == nil {
		return nil, fmt.Errorf("Plan did not include Sql Statement?")
	}
	if p.Stmt.Source == nil {
		return nil, fmt.Errorf("Plan did not include Sql Select Statement?")
	}
	if m.p == nil {
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

	m.ql = &rel.FilterStatement{}
	m.ql.From = m.sel.From[0].Name

	if m.sel.Where != nil {
		m.ql.Filter = m.sel.Where.Expr
	} else {
		m.ql.Filter = filterAll
	}

	reader := NewResultReader(m)
	p.Complete = false
	return reader, nil
}
