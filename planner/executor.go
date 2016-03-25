package planner

import (
	"fmt"

	u "github.com/araddon/gou"
	"github.com/lytics/grid"

	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/plan"
	"github.com/araddon/qlbridge/schema"
)

var (
	// ensure it meets interfaces
	_ exec.Executor = (*ExecutorGrid)(nil)

	_ = u.EMPTY
)

// Build a Sql Job which may be a Grid/Distributed job
func BuildSqlJob(ctx *plan.Context, gs *Server) (*ExecutorGrid, error) {
	sqlPlanner := plan.NewPlanner(ctx)
	baseJob := exec.NewExecutor(ctx, sqlPlanner)

	job := &ExecutorGrid{JobExecutor: baseJob}
	baseJob.Executor = job
	job.GridServer = gs
	job.Ctx = ctx
	u.Debugf("buildsqljob: %T p:%p  %T p:%p", job, job, job.Executor, job.JobExecutor)
	//u.Debugf("buildsqljob2: %T  %T", baseJob, baseJob.Executor)
	task, err := exec.BuildSqlJobPlanned(job.Planner, job, ctx)
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

// Build a Sql Job which may be a Grid/Distributed job
func BuildExecutorUnPlanned(ctx *plan.Context, gs *Server) (*ExecutorGrid, error) {

	baseJob := exec.NewExecutor(ctx, nil)

	job := &ExecutorGrid{JobExecutor: baseJob}
	baseJob.Executor = job
	job.GridServer = gs
	job.Ctx = ctx
	//u.Infof("Grid Actor Executor: %T p:%p  %T p:%p", job, job, job.Executor, job.JobExecutor)
	return job, nil
}

// Sql job that wraps the generic qlbridge job builder
// - contains ref to the shared GridServer which has info to
//   distribute tasks across servers
type ExecutorGrid struct {
	*exec.JobExecutor
	GridServer *Server
}

// Finalize is after the Dag of Relational-algebra tasks have been assembled
//  and just before we run them.
func (m *ExecutorGrid) Finalize(resultWriter exec.Task) error {

	m.JobExecutor.RootTask.Add(resultWriter)
	m.JobExecutor.Setup()
	//u.Debugf("planner.Finalize  %#v", m.JobExecutor.RootTask)
	//u.Debugf("finished finalize")
	return nil
}

func (m *ExecutorGrid) WalkSource(p *plan.Source) (exec.Task, error) {
	//u.Debugf("%p %T  NewSource? HasConn?%v SourceExec=%v", m, m, p.Conn != nil, p.SourceExec)
	if p.Conn != nil {
		e, hasSourceExec := p.Conn.(exec.ExecutorSource)
		if hasSourceExec {
			return e.WalkExecSource(p)
		}
	}
	return exec.NewSource(m.Ctx, p)
}
func (m *ExecutorGrid) WalkGroupBy(p *plan.GroupBy) (exec.Task, error) {
	u.Warnf("partial groupby")
	p.Partial = true
	return exec.NewGroupBy(m.Ctx, p), nil
}
func (m *ExecutorGrid) WalkSelect(p *plan.Select) (exec.Task, error) {
	if len(p.Stmt.From) > 0 {
		//u.Debugf("ExecutorGrid.WalkSelect ?  %s", p.Stmt.Raw)
	}

	//u.WarnT(10)
	if !p.ChildDag && len(p.Stmt.With) > 0 && p.Stmt.With.Bool("distributed") {
		//u.Warnf("%p has distributed!!!!!: %#v", m, p.Stmt.With)

		// We are going to run tasks remotely, so need a local grid source for them
		//  remoteSink  -> nats ->  localSource
		localTask := exec.NewTaskSequential(m.Ctx)
		taskUint, err := NextId()
		if err != nil {
			u.Errorf("Could not create task id %v", err)
			return nil, err
		}

		flow := NewFlow(taskUint)
		rx, err := grid.NewReceiver(m.GridServer.Grid.Nats(), flow.Name(), 2, 0)
		if err != nil {
			u.Errorf("%v: error: %v", "ourtask", err)
			return nil, err
		}
		natsSource := NewSourceNats(m.Ctx, rx)
		localTask.Add(natsSource)

		u.Infof("isAgg? %v", p.Stmt.IsAggQuery())
		if p.Stmt.IsAggQuery() {
			u.Debugf("Adding aggregate/group by?")
			gbplan := plan.NewGroupBy(p.Stmt)
			gb := exec.NewGroupByFinal(m.Ctx, gbplan)
			localTask.Add(gb)
		}

		// submit task in background node
		go func() {
			// task submission to worker actors
			if err := m.GridServer.SubmitTask(localTask, flow, p); err != nil {
				u.Errorf("Could not run task", err)
			}
			// need to send signal to quit
			ch := natsSource.MessageOut()
			u.Warnf("closing Source due to a task (first?) completing")
			close(ch)
			localTask.Close()
		}()
		return localTask, nil
	}
	//u.Warnf("%p  %p childdag? %v  %v", m, p, p.ChildDag, p.Stmt.String())
	return m.JobExecutor.WalkSelect(p)
}
func (m *ExecutorGrid) WalkSelectPartition(p *plan.Select, part *schema.Partition) (exec.Task, error) {

	//u.Infof("WTF:  %#v", m.JobExecutor)
	//u.Infof("%p  %p childdag? %v", m, p, p.ChildDag)
	return m.JobExecutor.WalkSelect(p)
}
