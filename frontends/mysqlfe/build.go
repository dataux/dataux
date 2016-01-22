package mysqlfe

import (
	"fmt"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/plan"
	"github.com/araddon/qlbridge/rel"

	"github.com/dataux/dataux/planner"
)

var (
	_ = u.EMPTY

	// ensure it meets visitor
	_ rel.Visitor = (*MySqlJob)(nil)
)

// Mysql job that wraps the generic qlbridge job builder
type MySqlJob struct {
	*exec.JobBuilder
	runner exec.TaskRunners
}

// Create a MySql job:  QlBridge jobs can be wrapper
//   allowing per-method (VisitShow etc) to be replaced by a dialect
//   specific handler.
// - mysql `SHOW CREATE TABLE name` is dialect specific so needs to be replaced
func BuildMySqlob(ctx *plan.Context) (*MySqlJob, error) {

	b := exec.JobBuilder{}
	b.Ctx = ctx
	// We are going to swap out a new distributed planner
	b.TaskMaker = planner.TaskRunnersMaker
	job := &MySqlJob{JobBuilder: &b}
	b.Visitor = job

	task, err := exec.BuildSqlJobVisitor(job, ctx)
	taskRunner, ok := task.(exec.TaskRunner)
	if !ok {
		return nil, fmt.Errorf("Expected TaskRunner but was %T", task)
	}

	job.RootTask = taskRunner

	if job.Ctx.Projection != nil {
		return job, nil
	}

	if sqlSelect, ok := job.Ctx.Stmt.(*rel.SqlSelect); ok {
		job.Ctx.Projection, err = plan.NewProjectionFinal(ctx, sqlSelect)
		//u.Debugf("load projection final job.Projection: %p", job.Projection)
		if err != nil {
			return nil, err
		}
	}

	return job, err
}
