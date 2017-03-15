package lytics

import (
	"fmt"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/plan"
	"github.com/araddon/qlbridge/rel"
	"github.com/araddon/qlbridge/schema"
)

var (
	DefaultLimit = 1000

	// Implement Push Down Planner interface, this allows
	// the Generator do do its own planning, taking over from
	// default sql planner as we are going to push down as
	// much as possible to lytics api
	_ plan.SourcePlanner  = (*Generator)(nil)
	_ exec.ExecutorSource = (*Generator)(nil)
)

// Generator Sql To Lytics Request Generator
//   Map sql queries into Lytics Api Requests
type Generator struct {
	resp          *ResultReader
	p             *plan.Source
	ql            *rel.FilterStatement
	tbl           *schema.Table
	sel           *rel.SqlSelect
	schema        *schema.SchemaSource
	ctx           *plan.Context
	partition     *schema.Partition // current partition for this request
	needsPolyFill bool              // do we request that features be polyfilled?
	apiKey        string
}

func NewGenerator(table *schema.Table, apiKey string) *Generator {
	return &Generator{
		tbl:    table,
		schema: table.SchemaSource,
		apiKey: apiKey,
	}
}

func (m *Generator) Close() error { return nil }

// WalkSourceSelect is used during planning phase, to create a plan (plan.Task)
//  or error, and to report back any poly-fill necessary
func (m *Generator) WalkSourceSelect(planner plan.Planner, p *plan.Source) (plan.Task, error) {
	m.p = p
	return nil, nil
}

func (m *Generator) WalkExecSource(p *plan.Source) (exec.Task, error) {

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
		m.ql = ql
		u.Infof("query %v", ql)
	} else {
		u.Warnf("NO WHERE/FILTER? %v", m.sel)
	}

	reader := NewResultReader(m)
	p.Complete = false
	return reader, nil
}
