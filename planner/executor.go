package planner

import (
	"fmt"

	u "github.com/araddon/gou"
	"github.com/lytics/grid/grid.v2"

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
func BuildSqlJob(ctx *plan.Context, pg *PlannerGrid) (*ExecutorGrid, error) {
	sqlPlanner := plan.NewPlanner(ctx)
	baseJob := exec.NewExecutor(ctx, sqlPlanner)

	job := &ExecutorGrid{JobExecutor: baseJob}
	baseJob.Executor = job
	job.GridServer = pg
	job.Ctx = ctx
	u.Debugf("buildsqljob: %T p:%p  %T p:%p", job, job, job.Executor, job.JobExecutor)
	if pg == nil {
		u.Warnf("Grid Server Doesn't exist %v", pg)
	} else if pg.Grid == nil {
		u.Warnf("Grid doens't exist? ")
	} else if pg.Grid.Nats() == nil {
		u.Warnf("Grid.Nats() doesnt exist")
	}
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

// Build a Sql Job which has already been planned so this is just execution runner
func BuildExecutorUnPlanned(ctx *plan.Context, pg *PlannerGrid) (*ExecutorGrid, error) {

	baseJob := exec.NewExecutor(ctx, nil)

	job := &ExecutorGrid{JobExecutor: baseJob}
	baseJob.Executor = job
	job.GridServer = pg
	if pg == nil {
		u.Warnf("nope, need a grid server ")
		//return nil, fmt.Errorf("no grid server")
	}
	job.Ctx = ctx
	//u.Infof("Grid Actor Executor: %T p:%p  %T p:%p", job, job, job.Executor, job.JobExecutor)
	return job, nil
}

// Sql job that wraps the generic qlbridge job builder
// - contains ref to the shared GridServer which has info to
//   distribute tasks across servers
type ExecutorGrid struct {
	*exec.JobExecutor
	distributed bool
	sp          *plan.Select
	GridServer  *PlannerGrid
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
	if len(p.Static) > 0 {
		return m.JobExecutor.WalkSource(p)
	}
	if p.Conn != nil {
		e, hasSourceExec := p.Conn.(exec.ExecutorSource)
		if hasSourceExec {
			return e.WalkExecSource(p)
		}
	}
	return exec.NewSource(m.Ctx, p)
}

// func (m *ExecutorGrid) WalkProjection(p *plan.Projection) (exec.Task, error) {
// 	u.Debugf("%p Walk Projection  sp:%+v", m, m.sp)
// 	return exec.NewProjection(m.Ctx, p), nil
// }
func (m *ExecutorGrid) WalkGroupBy(p *plan.GroupBy) (exec.Task, error) {

	if m.distributed {
		//u.Debugf("%p partial groupby distributed? %v", m, m.distributed)
		p.Partial = true
	}
	return exec.NewGroupBy(m.Ctx, p), nil
}
func (m *ExecutorGrid) WalkSelect(p *plan.Select) (exec.Task, error) {
	if len(p.Stmt.From) > 0 {
		//u.Debugf("ExecutorGrid.WalkSelect ?  %s", p.Stmt.Raw)
	}

	//u.WarnT(10)
	//u.Debugf("%p Walk Select %s", m, p.Stmt)
	if !p.ChildDag && len(p.Stmt.With) > 0 && p.Stmt.With.Bool("distributed") {
		u.Warnf("%p has distributed!!!!!: %#v", m, p.Stmt.With)
		// We are going to run tasks remotely, so need a local grid source for them
		//  remoteSink  -> nats ->  localSource
		localTask := exec.NewTaskSequential(m.Ctx)
		localTask.Name = "DistributedMaster"
		taskUint, err := NextId()
		if err != nil {
			u.Errorf("Could not create task id %v", err)
			return nil, err
		}
		gs := m.GridServer
		if gs == nil {
			u.Warnf("Grid Server Doesn't exist %v", gs)
		} else if gs.Grid == nil {
			u.Warnf("Grid doens't exist? ")
		} else if gs.Grid.Nats() == nil {
			u.Warnf("Grid.Nats() doesnt exist")
		}

		flow := NewFlow(taskUint)
		rx, err := grid.NewReceiver(m.GridServer.Grid.Nats(), flow.Name(), 2, 0)
		if err != nil {
			u.Errorf("%p grid receiver start %s err=%v", m, flow.Name(), err)
			return nil, err
		}
		natsSource := NewSourceNats(m.Ctx, rx)
		localTask.Add(natsSource)
		var completionTask exec.TaskRunner

		// For aggregations, group-by, or limit clauses we will need to do final
		// aggregation here in master as the reduce step
		if p.Stmt.IsAggQuery() {
			//u.Debugf("Adding aggregate/group by?")
			gbplan := plan.NewGroupBy(p.Stmt)
			gb := exec.NewGroupByFinal(m.Ctx, gbplan)
			localTask.Add(gb)
			completionTask = gb
		} else if p.NeedsFinalProjection() {
			projplan, err := plan.NewProjectionFinal(m.Ctx, p)
			if err != nil {
				u.Errorf("%p projection final error %s err=%v", m, flow.Name(), err)
				return nil, err
			}
			proj := exec.NewProjectionLimit(m.Ctx, projplan)
			localTask.Add(proj)
			completionTask = proj
		} else {
			completionTask = localTask
		}

		// submit query execution tasks to run on other worker nodes
		go func() {
			//
			if err := m.GridServer.RunSqlMaster(completionTask, natsSource, flow, p); err != nil {
				u.Errorf("Could not run task", err)
			}
			//u.Debugf("%p closing Source due to a task (first?) completing", m)
			//time.Sleep(time.Millisecond * 30)
			// If we close input source, then will it shutdown the rest?
			natsSource.Close()
			//u.Debugf("%p after closing NatsSource %p", m, natsSource)
		}()
		return localTask, nil
	}
	//u.Warnf("%p  %p childdag? %v  %v", m, p, p.ChildDag, p.Stmt.String())
	return m.JobExecutor.WalkSelect(p)
}

// WalkSelectPartition is ONLY called by child-dag's, ie the remote end of a distributed
//  sql query, to allow setup before walking
func (m *ExecutorGrid) WalkSelectPartition(p *plan.Select, part *schema.Partition) (exec.Task, error) {
	//u.Infof("%p  %p childdag? %v", m, p, p.ChildDag)
	m.sp = p
	m.distributed = true
	return m.JobExecutor.WalkSelect(p)
}
