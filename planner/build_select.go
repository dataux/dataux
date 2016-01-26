package planner

import (
	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/rel"
	"github.com/dataux/dataux/planner/gridrunner"
)

var (
	_ = u.EMPTY

	_ rel.SourceVisitor = (*SourceBuilder)(nil)
)

type SourceBuilder struct {
	*exec.SourceBuilder
	Grid *gridrunner.Server
}

func NewSourceBuilder(esb *exec.SourceBuilder, g *gridrunner.Server) *SourceBuilder {
	sb := SourceBuilder{}
	sb.SourceBuilder = esb
	sb.Grid = g
	return &sb
}

func (m *SqlJob) VisitSelect(stmt *rel.SqlSelect) (rel.Task, rel.VisitStatus, error) {
	if len(stmt.From) > 0 {
		u.Debugf("planner.VisitSelect ?  %s", stmt.Raw)
	}
	return m.JobBuilder.VisitSelect(stmt)
}

/*
// Interface for sub-select Tasks of the Select Statement
type SourceVisitor interface {
	VisitSourceSelect() (Task, VisitStatus, error)
	VisitSource(scanner interface{} / *schema.Scanner* /) (Task, VisitStatus, error)
	VisitSourceJoin(scanner interface{} / *schema.Scanner* /) (Task, VisitStatus, error)
	VisitWhere() (Task, VisitStatus, error)
}
*/
func (m *SourceBuilder) VisitSourceSelect() (rel.Task, rel.VisitStatus, error) {

	sp := m.Plan
	u.Debugf("planner.SourceBuilder.VisitSourceSelect() %#v", sp)
	if sp.From.Source != nil && len(sp.From.Source.With) > 0 && sp.From.Source.With.Bool("distributed") {
		u.Warnf("has distributed!!!!!: %#v", sp.From.Source.With)
		m.Grid.SubmitTask(sp)
	}

	return m.SourceBuilder.VisitSourceSelect()
}
