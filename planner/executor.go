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
	planner := plan.NewPlanner(ctx)
	baseJob := exec.NewExecutor(ctx, planner)

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
	u.Debugf("planner.Finalize")

	m.JobExecutor.RootTask.Add(resultWriter)
	m.JobExecutor.Setup()
	return nil
}

func (m *ExecutorGrid) WalkSelect(p *plan.Select, root exec.Task) (exec.Task, error) {
	if len(p.Stmt.From) > 0 {
		u.Debugf("planner.VisitSelect ?  %s", p.Stmt.Raw)
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

func (m *ExecutorGrid) WalkPlan(p plan.Task) (exec.Task, error) {
	var root exec.Task
	if p.IsParallel() {
		root = exec.NewTaskParallel(m.Ctx)
	} else {
		root = exec.NewTaskSequential(m.Ctx)
	}
	switch pt := p.(type) {
	case *plan.Select:
		return root, m.WalkChildren(pt, root)
		// case *plan.PreparedStatement:
	case *plan.Upsert:
		return root, root.Add(exec.NewUpsert(m.Ctx, pt))
	case *plan.Insert:
		return root, root.Add(exec.NewInsert(m.Ctx, pt))
	case *plan.Update:
		return root, root.Add(exec.NewUpdate(m.Ctx, pt))
	case *plan.Delete:
		return root, root.Add(exec.NewDelete(m.Ctx, pt))
		// case *plan.Show:
		// case *plan.Describe:
		// case *plan.Command:
	}
	panic(fmt.Sprintf("Not implemented for %T", p))
}
func (m *ExecutorGrid) WalkPlanAll(p plan.Task) (exec.Task, error) {
	root, err := m.WalkPlanTask(p)
	if err != nil {
		u.Errorf("all damn %v err=%v", p, err)
		return nil, err
	}
	if len(p.Children()) > 0 {
		var dagRoot exec.Task
		u.Debugf("sequential?%v  parallel?%v", p.IsSequential(), p.IsParallel())
		if p.IsParallel() {
			dagRoot = exec.NewTaskParallel(m.Ctx)
		} else {
			dagRoot = exec.NewTaskSequential(m.Ctx)
		}
		err = dagRoot.Add(root)
		if err != nil {
			u.Errorf("Could not add root: %v", err)
			return nil, err
		}
		return dagRoot, m.WalkChildren(p, dagRoot)
	}
	u.Debugf("got root? %T for %T", root, p)
	u.Debugf("len=%d  for children:%v", len(p.Children()), p.Children())
	return root, m.WalkChildren(p, root)
}
func (m *ExecutorGrid) WalkPlanTask(p plan.Task) (exec.Task, error) {
	switch pt := p.(type) {
	case *plan.Source:
		return exec.NewSource(pt)
	case *plan.Where:
		return exec.NewWhere(m.Ctx, pt), nil
	case *plan.Having:
		return exec.NewHaving(m.Ctx, pt), nil
	case *plan.GroupBy:
		return exec.NewGroupBy(m.Ctx, pt), nil
	case *plan.Projection:
		return exec.NewProjection(m.Ctx, pt), nil
	case *plan.JoinMerge:
		return m.WalkJoin(pt)
	case *plan.JoinKey:
		return exec.NewJoinKey(m.Ctx, pt), nil
	}
	panic(fmt.Sprintf("Task plan-exec Not implemented for %T", p))
}
func (m *ExecutorGrid) WalkChildren(p plan.Task, root exec.Task) error {
	for _, t := range p.Children() {
		u.Debugf("parent: %T  walk child %T", p, t)
		et, err := m.WalkPlanTask(t)
		if err != nil {
			u.Errorf("could not create task %#v err=%v", t, err)
		}
		if len(t.Children()) > 0 {
			u.Warnf("has children but not handled %#v", t)
		}
		err = root.Add(et)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *ExecutorGrid) WalkJoin(p *plan.JoinMerge) (exec.Task, error) {
	execTask := exec.NewTaskParallel(m.Ctx)
	u.Debugf("join.Left: %#v    \nright:%#v", p.Left, p.Right)
	l, err := m.WalkPlanAll(p.Left)
	if err != nil {
		u.Errorf("whoops %T  %v", l, err)
		return nil, err
	}
	err = execTask.Add(l)
	if err != nil {
		u.Errorf("whoops %T  %v", l, err)
		return nil, err
	}
	r, err := m.WalkPlanAll(p.Right)
	if err != nil {
		return nil, err
	}
	err = execTask.Add(r)
	if err != nil {
		return nil, err
	}

	jm := exec.NewJoinNaiveMerge(m.Ctx, l.(exec.TaskRunner), r.(exec.TaskRunner), p)
	err = execTask.Add(jm)
	if err != nil {
		return nil, err
	}
	return execTask, nil
}
