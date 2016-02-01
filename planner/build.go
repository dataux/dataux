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
	_ plan.Visitor = (*SqlJob)(nil)

	_ = u.EMPTY
)

// Sql job that wraps the generic qlbridge job builder
type SqlJob struct {
	*exec.JobBuilder
	GridServer *gridrunner.Server
	Visitor    plan.Visitor
	runner     exec.TaskRunners
}

// Finalize is after the Dag of Relational-algebra tasks have been assembled
//  and just before we run them.
//
func (m *SqlJob) Finalize(resultWriter plan.Task) error {
	u.Debugf("planner.Finalize")

	// switch stmt := m.JobBuilder.Ctx.Stmt.(type) {
	// case *rel.SqlSelect:
	// 	u.Warnf("planner.Finalize is SELECT, distributeable %s", stmt)
	// }

	// Entire Job so far is going to be run elsewhere, then we are going to create
	// a stub here to receive via nats the rsults and write out.
	m.JobBuilder.RootTask.Add(resultWriter)
	m.JobBuilder.Setup()
	return nil
}

// Many of the ShowMethods are dialect specific so this method will be replaced
func (m *SqlJob) VisitShow(sp *plan.Show) (plan.Task, rel.VisitStatus, error) {
	//u.Debugf("planner.VisitShow create?%v  identity=%q  raw=%s", sp.Stmt.Create, sp.Stmt.Identity, sp.Stmt.Raw)
	return m.JobBuilder.VisitShow(sp)
}
