package planner

import (
	"fmt"
	"time"

	u "github.com/araddon/gou"

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
	} else if pg.GridServer == nil {
		u.Warnf("Grid doens't exist? ")
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

	u.Debugf("IS DISTRIBUTED!!!!")
	return exec.NewGroupBy(m.Ctx, p), nil
}
func (m *ExecutorGrid) WalkSelect(p *plan.Select) (exec.Task, error) {

	//u.WarnT(10)
	//u.Debugf("%p Walk Select %s", m, p.Stmt)
	if !p.ChildDag && len(p.Stmt.With) > 0 && p.Stmt.With.Bool("distributed") {
		u.Debugf("%p has distributed!!!!!: %#v", m, p.Stmt.With)
		time.Sleep(time.Millisecond * 1000)
		// We are going to run tasks remotely, so need a local grid source for them
		// remoteSink  -> nats ->  localSource
		localTask := exec.NewTaskSequential(m.Ctx)
		localTask.Name = "DistributedMaster"
		// taskUint, err := NextId()
		// if err != nil {
		// 	u.Errorf("Could not create task id %v", err)
		// 	return nil, err
		// }
		gs := m.GridServer
		if gs == nil {
			u.Warnf("Grid Server Doesn't exist %v", gs)
		} else if gs.GridServer == nil {
			u.Warnf("Grid doens't exist? ")
		}

		// Checkout a Source from a Pool of Mailboxes
		// This is a blocking request, if we have depleted our pool of
		// mailboxes this will block
		mbox, err := m.GridServer.GetMailbox()
		if err != nil {
			u.Errorf("Could not get mailbox %v", err)
			return nil, err
		}
		txferSource := NewSource(m.Ctx, mbox.Id(), mbox.C)
		localTask.Add(txferSource)

		var completionTask exec.TaskRunner

		// For aggregations, group-by, or limit clauses we will need to do final
		// aggregation here in master as the reduce step
		if p.Stmt.IsAggQuery() {
			u.Debugf("GRID PLANNER Adding aggregate/group by? %s", p.Stmt)
			gbplan := plan.NewGroupBy(p.Stmt)
			gb := exec.NewGroupByFinal(m.Ctx, gbplan)
			localTask.Add(gb)
			completionTask = gb
		} else if p.NeedsFinalProjection() {
			projplan, err := plan.NewProjectionFinal(m.Ctx, p)
			if err != nil {
				u.Errorf("%p projection final error %s err=%v", m, mbox.Id(), err)
				return nil, err
			}
			proj := exec.NewProjectionLimit(m.Ctx, projplan)
			localTask.Add(proj)
			completionTask = proj
		} else {
			completionTask = localTask
		}

		// Create our distributed sql task
		task := newSqlMasterTask(m.GridServer, completionTask, txferSource, p)
		err = task.init()
		if err != nil {
			u.Errorf("Could not setup task", err)
			return nil, err
		}

		// submit query execution tasks to run on other worker nodes
		go func() {

			u.Warnf("About to Run the task %d", task.actorCt)
			if err = task.Run(); err != nil {
				u.Errorf("Could not run task")
			}
			m.GridServer.CheckinMailbox(mbox)
			//u.Debugf("%p closing Source due to a task (first?) completing", m)
			//time.Sleep(time.Millisecond * 30)
			// If we close input source, then will it shutdown the rest?
			txferSource.Close()
			//u.Debugf("%p after closing NatsSource %p", m, txferSource)
		}()
		return localTask, nil
	}
	//u.Warnf("%p  %p childdag? %v  %v", m, p, p.ChildDag, p.Stmt.String())
	return m.JobExecutor.WalkSelect(p)
}

// WalkSelectPartition is ONLY called by child-dag's, ie the remote end of a distributed
//  sql query, to allow setup before walking
func (m *ExecutorGrid) WalkSelectPartition(p *plan.Select, part *schema.Partition) (exec.Task, error) {
	u.Infof("%p  %p Exec:%T  ChildDag?%v", m, p, m.JobExecutor, p.ChildDag)
	m.sp = p
	m.distributed = true
	return m.JobExecutor.WalkSelect(p)
}
