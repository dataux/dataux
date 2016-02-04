package planner

import (
	"fmt"

	u "github.com/araddon/gou"
	"github.com/lytics/grid"

	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/plan"

	"github.com/dataux/dataux/planner/gridrunner"
)

var (
	// ensure it meets interfaces
	_ exec.Executor = (*ExecutorGrid)(nil)

	_ = u.EMPTY
)

func BuildSqlJob(ctx *plan.Context, Grid *gridrunner.Server) (*ExecutorGrid, error) {
	sqlPlanner := plan.NewPlanner(ctx)
	baseJob := exec.NewExecutor(ctx, sqlPlanner)

	job := &ExecutorGrid{JobExecutor: baseJob}
	job.Executor = job
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
	return job, err
}

// Sql job that wraps the generic qlbridge job builder
type ExecutorGrid struct {
	*exec.JobExecutor
	GridServer *gridrunner.Server
}

// Finalize is after the Dag of Relational-algebra tasks have been assembled
//  and just before we run them.
//
func (m *ExecutorGrid) Finalize(resultWriter exec.Task) error {
	u.Debugf("planner.Finalize  %#v", m.JobExecutor.RootTask)

	m.JobExecutor.RootTask.Add(resultWriter)
	m.JobExecutor.Setup()
	u.Debugf("finished finalize")
	return nil
}

// func (m *ExecutorGrid) WalkSelect(p *plan.Select) (exec.Task, error) {
// 	root := m.NewTask(p)
// 	return root, m.WalkChildren(p, root)
// }

func (m *ExecutorGrid) WalkSelect2(p *plan.Select, root exec.Task) (exec.Task, error) {
	if len(p.Stmt.From) > 0 {
		u.Debugf("ExecutorGrid.WalkSelect ?  %s", p.Stmt.Raw)
	}

	if len(p.Stmt.With) > 0 && p.Stmt.With.Bool("distributed") {
		u.Warnf("has distributed!!!!!: %#v", p.Stmt.With)

		localSink := exec.NewTaskSequential(m.Ctx)
		taskUint, err := gridrunner.NextId()
		if err != nil {
			u.Errorf("Could not create task id %v", err)
			return nil, err
		}
		flow := gridrunner.NewFlow(taskUint)

		rx, err := grid.NewReceiver(m.GridServer.Grid.Nats(), flow.Name(), 2, 0)
		if err != nil {
			u.Errorf("%v: error: %v", "ourtask", err)
			return nil, err
		}
		natsSource := NewSourceNats(m.Ctx, rx)
		localSink.Add(natsSource)

		tx, err := grid.NewSender(m.GridServer.Grid.Nats(), 1)
		if err != nil {
			u.Errorf("error: %v", err)
		}
		natsSink := NewSinkNats(m.Ctx, flow.Name(), tx)
		root.Add(natsSink)

		// submit task in background node
		go func() {
			m.GridServer.SubmitTask(localSink, flow, root, p) // task submission to worker actors
			// need to send signal to quit
			ch := natsSource.MessageOut()
			close(ch)
		}()

		return localSink, nil
	}
	return nil, nil
}
