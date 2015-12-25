package mysqlfe

import (
	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/plan"
)

var (
	_ = u.EMPTY

	// ensure it meets visitor
	_ expr.Visitor = (*MySqlJob)(nil)
)

// Create a MySql job:  QlBridge jobs can be wrapper
//   allowing per-method (VisitShow etc) to be replaced by a dialect
//   specific handler.
// - mysql `SHOW CREATE TABLE name` is dialect specific so needs to be replaced
func BuildMySqlob(ctx *plan.Context) (*exec.SqlJob, error) {
	job := NewMySqlJob(ctx)
	return exec.BuildSqlProjectedWrapper(job, ctx)
}

// Mysql job that wraps the generic sql job
//  - so it can claim the Show/Create statements
type MySqlJob struct {
	expr.Visitor
	Ctx      *plan.Context
	children exec.Tasks
}

// JobBuilder
func NewMySqlJob(ctx *plan.Context) *MySqlJob {
	b := MySqlJob{}
	b.Ctx = ctx
	return &b
}

// Implement the Wrap() part of visitor interface
// - this MySqlJob will be called, and if it has not-hidden
//    it will be deferred to embedded visitor here
func (m *MySqlJob) Wrap(visitor expr.Visitor) expr.Visitor {
	//u.Errorf("YAY! wrap %T", visitor)
	m.Visitor = visitor
	return m
}
