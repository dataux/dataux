package mysqlfe

import (
	"fmt"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/plan"

	"github.com/dataux/dataux/models"
	"github.com/dataux/dataux/planner"
)

var (
	_ = u.EMPTY

	// ensure it meets visitor
	_ exec.Executor = (*MySqlJob)(nil)
)

// MySqlJob job that wraps the dataux distributed planner with a dialect specific one
type MySqlJob struct {
	*planner.GridTask
}

// BuildMySqlJob Create a MySql job that wraps underlying distributed planner, and qlbridge generic implementation
//   allowing per-method (VisitShow etc) to be replaced by a dialect specific handler.
// - mysql `SHOW CREATE TABLE name` for example is dialect specific so needs to be replaced
// - also wraps a distributed planner from dataux
func BuildMySqlJob(svr *models.ServerCtx, ctx *plan.Context) (*MySqlJob, error) {

	// Ensure it parses, right now we can't handle multiple statement (ie with semi-colons separating)
	// sql = strings.TrimRight(sql, ";")
	jobPlanner := plan.NewPlanner(ctx)
	baseJob := exec.NewExecutor(ctx, jobPlanner)

	job := &planner.GridTask{JobExecutor: baseJob}
	job.Executor = job
	job.GridServer = svr.PlanGrid
	job.Ctx = ctx
	task, err := exec.BuildSqlJobPlanned(job.Planner, job.Executor, ctx)
	if err != nil {
		return nil, err
	}
	taskRunner, ok := task.(exec.TaskRunner)
	if !ok {
		return nil, fmt.Errorf("Expected TaskRunner but was %T", task)
	}
	job.RootTask = taskRunner
	return &MySqlJob{job}, err
}
