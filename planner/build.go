package planner

import (
	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/plan"
	"github.com/araddon/qlbridge/rel"

	"github.com/dataux/dataux/planner/gridrunner"
)

var (
	// ensure it meets visitor
	_ rel.Visitor = (*SqlJob)(nil)

	_ = u.EMPTY
)

// Sql job that wraps the generic qlbridge job builder
type SqlJob struct {
	*exec.JobBuilder
	Grid    *gridrunner.Server
	Visitor rel.Visitor
	runner  exec.TaskRunners
}

// Many of the ShowMethods are MySql dialect specific so will be replaced here
func (m *SqlJob) Finalize(resultWriter plan.Task) error {
	u.Debugf("planner.Finalize")
	m.JobBuilder.RootTask.Add(resultWriter)
	m.JobBuilder.Setup()
	return nil
}

// Many of the ShowMethods are MySql dialect specific so will be replaced here
func (m *SqlJob) VisitShow(stmt *rel.SqlShow) (rel.Task, rel.VisitStatus, error) {
	u.Debugf("planner.VisitShow create?%v  identity=%q  raw=%s", stmt.Create, stmt.Identity, stmt.Raw)
	return m.JobBuilder.VisitShow(stmt)
}
