package mysqlfe

import (
	"fmt"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/plan"
	"github.com/araddon/qlbridge/rel"

	"github.com/dataux/dataux/models"
	"github.com/dataux/dataux/planner"
)

var (
	_ = u.EMPTY

	// ensure it meets visitor
	_ rel.Visitor = (*MySqlJob)(nil)
)

// Mysql job that wraps the generic qlbridge job builder
type MySqlJob struct {
	*planner.SqlJob
}

// Create a MySql job that wraps underlying qlbridge generic implementation
//   allowing per-method (VisitShow etc) to be replaced by a dialect specific handler.
// - mysql `SHOW CREATE TABLE name` for example is dialect specific so needs to be replaced
// - also allows a distributed planner from dataux
func BuildMySqlob(svr *models.ServerCtx, ctx *plan.Context) (*MySqlJob, error) {

	b := exec.JobBuilder{}
	b.Ctx = ctx
	// We are going to replace qlbridge planner with dataux distributed one
	b.TaskMaker = planner.TaskRunnersMaker(ctx, svr.Grid)
	job := &planner.SqlJob{JobBuilder: &b, Grid: svr.Grid}
	mj := &MySqlJob{SqlJob: job}
	job.Visitor = mj
	b.Visitor = mj

	//u.Debugf("SqlJob:%p exec.Job:%p about to build: %#v", job, &b, mj)
	task, err := exec.BuildSqlJobVisitor(mj, ctx)
	if err != nil {
		return nil, err
	}
	taskRunner, ok := task.(exec.TaskRunner)
	if !ok {
		return nil, fmt.Errorf("Expected TaskRunner type root task but was %T", task)
	}

	job.RootTask = taskRunner

	if job.Ctx.Projection != nil {
		return mj, nil
	}

	if sqlSelect, ok := job.Ctx.Stmt.(*rel.SqlSelect); ok {
		job.Ctx.Projection, err = plan.NewProjectionFinal(ctx, sqlSelect)
		//u.Debugf("load projection final job.Projection: %p", job.Projection)
		if err != nil {
			return nil, err
		}
	}

	return mj, nil
}
