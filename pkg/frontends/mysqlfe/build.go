package mysqlfe

import (
	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/plan"
	"github.com/araddon/qlbridge/rel"
)

var (
	_ = u.EMPTY

	// ensure it meets visitor
	_ rel.Visitor = (*MySqlJob)(nil)
)

// Create a MySql job:  QlBridge jobs can be wrapper
//   allowing per-method (VisitShow etc) to be replaced by a dialect
//   specific handler.
// - mysql `SHOW CREATE TABLE name` is dialect specific so needs to be replaced
func BuildMySqlob(ctx *plan.Context) (*exec.JobBuilder, error) {
	job := NewMySqlJob(ctx)
	//u.Debugf("about to call exec.buildSqlProjectedWrapper: %p", job)
	qlbjob, err := exec.BuildSqlProjected(job, ctx)
	//u.Debugf("%p for sqljob? P:%p  %+v", job, qlbjob, qlbjob)
	//u.Warnf("planner? %#v", qlbjob.Planner)
	//job.SqlJob = qlbjob
	return qlbjob, err
}

// Mysql job that wraps the generic sql job
//  - so it can claim the Show/Create statements
type MySqlJob struct {
	rel.Visitor
	TaskMaker plan.TaskMaker
	//SqlJob    *exec.JobBuilder
	Ctx    *plan.Context
	runner exec.TaskRunners
}

// JobBuilder
func NewMySqlJob(ctx *plan.Context) *MySqlJob {
	b := MySqlJob{}
	b.Ctx = ctx
	b.TaskMaker = exec.TaskRunnersMaker
	return &b
}

// Implement the Wrap() part of visitor interface
// - this MySqlJob will be called, and if it has not-hidden
//    it will be deferred to embedded visitor here
func (m *MySqlJob) Wrap(visitor rel.Visitor) rel.Visitor {
	//u.Warnf("%p  visitior wrap %p  %T", m, visitor, visitor)
	m.Visitor = visitor
	return m
}
