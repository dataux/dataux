package planner

import (
	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/plan"

	"github.com/dataux/dataux/planner/gridrunner"
)

var (
	_ = u.EMPTY

	// ensure we implement interface
	_ plan.TaskPlanner = (*TaskRunners)(nil)
)

type TaskRunners struct {
	ctx     *plan.Context
	tasks   []plan.Task
	runners []exec.TaskRunner
	Grid    *gridrunner.Server
}

func TaskRunnersMaker(ctx *plan.Context, g *gridrunner.Server) plan.TaskPlanner {
	return &TaskRunners{
		ctx:     ctx,
		tasks:   make([]plan.Task, 0),
		runners: make([]exec.TaskRunner, 0),
		Grid:    g,
	}
}
func (m *TaskRunners) SourceVisitorMaker(sp *plan.Source) plan.SourceVisitor {
	esb := exec.NewSourceBuilder(sp, m)
	sb := NewSourceBuilder(esb, m.Grid)
	sb.SourceVisitor = sb
	u.Infof("source visitor maker")
	return sb
}
func (m *TaskRunners) Sequential(name string) plan.Task {
	return exec.NewSequential(m.ctx, name)
}
func (m *TaskRunners) Parallel(name string) plan.Task {
	return exec.NewTaskParallel(m.ctx, name)
}
