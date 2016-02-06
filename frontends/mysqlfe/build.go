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

// Mysql job that wraps the dataux distributed planner with a dialect specific one
type MySqlJob struct {
	*planner.ExecutorGrid
}

// Create a MySql job that wraps underlying distributed planner, and qlbridge generic implementation
//   allowing per-method (VisitShow etc) to be replaced by a dialect specific handler.
// - mysql `SHOW CREATE TABLE name` for example is dialect specific so needs to be replaced
// - also wraps a distributed planner from dataux
func BuildMySqlJob(svr *models.ServerCtx, ctx *plan.Context) (*MySqlJob, error) {
	jobPlanner := plan.NewPlanner(ctx)
	baseJob := exec.NewExecutor(ctx, jobPlanner)

	job := &planner.ExecutorGrid{JobExecutor: baseJob}
	//baseJob.Executor = job  // ??
	job.Executor = job
	job.GridServer = svr.Grid
	u.Infof("executor: %T    sub.executor: %T", job, job.Executor)
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
