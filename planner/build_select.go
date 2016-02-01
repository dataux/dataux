package planner

import (
	u "github.com/araddon/gou"
	"github.com/lytics/grid"

	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/plan"
	"github.com/araddon/qlbridge/rel"
	"github.com/dataux/dataux/planner/gridrunner"
)

var (
	_ = u.EMPTY

	_ plan.SourceVisitor = (*SourceBuilder)(nil)
)

type SourceBuilder struct {
	*exec.SourceBuilder
	GridServer *gridrunner.Server
}

func NewSourceBuilder(esb *exec.SourceBuilder, gs *gridrunner.Server) *SourceBuilder {
	sb := SourceBuilder{}
	sb.SourceBuilder = esb
	sb.GridServer = gs
	return &sb
}

func (m *SqlJob) VisitSelect(sp *plan.Select) (plan.Task, rel.VisitStatus, error) {
	if len(sp.Stmt.From) > 0 {
		u.Debugf("planner.VisitSelect ?  %s", sp.Stmt.Raw)
	}

	t, status, err := m.JobBuilder.VisitSelect(sp)
	if err != nil {
		return t, status, err
	}
	if len(sp.Stmt.With) > 0 && sp.Stmt.With.Bool("distributed") {
		u.Warnf("has distributed!!!!!: %#v", sp.Stmt.With)

		localSink := m.TaskMaker.Sequential("distributed-sink")
		taskUint, err := gridrunner.NextId()
		if err != nil {
			u.Errorf("Could not create task id %v", err)
			return nil, rel.VisitError, err
		}
		flow := gridrunner.NewFlow(taskUint)

		rx, err := grid.NewReceiver(m.GridServer.Grid.Nats(), flow.Name(), 2, 0)
		if err != nil {
			u.Errorf("%v: error: %v", "ourtask", err)
			return nil, rel.VisitError, err
		}
		natsSource := NewSourceNats(m.Ctx, rx)
		localSink.Add(natsSource)

		localSinkRunner, ok := localSink.(exec.TaskRunner)
		if !ok {
			u.Errorf("Expected exec.TaskRunner but got %T", localSink)
			return nil, rel.VisitError, nil
		}

		tx, err := grid.NewSender(m.GridServer.Grid.Nats(), 1)
		if err != nil {
			u.Errorf("error: %v", err)
		}
		natsSink := NewSinkNats(m.Ctx, flow.Name(), tx)
		t.Add(natsSink)

		// submit task in background node
		go func() {
			m.GridServer.SubmitTask(localSinkRunner, flow, t, sp) // task submission to worker actors
			// need to send signal to quit
			ch := natsSource.MessageOut()
			close(ch)
		}()

		return localSink, rel.VisitContinue, nil
	}
	return t, status, err
}

/*
// Interface for sub-select Tasks of the Select Statement
type SourceVisitor interface {
	VisitSourceSelect() (Task, VisitStatus, error)
	VisitSource(scanner interface{} / *schema.Scanner* /) (Task, VisitStatus, error)
	VisitSourceJoin(scanner interface{} / *schema.Scanner* /) (Task, VisitStatus, error)
	VisitWhere() (Task, VisitStatus, error)
}
*/
func (m *SourceBuilder) VisitSourceSelect(sp *plan.Source) (plan.Task, rel.VisitStatus, error) {

	u.Debugf("planner.SourceBuilder.VisitSourceSelect() %#v", sp)

	t, status, err := m.SourceBuilder.VisitSourceSelect(sp)
	if err != nil {
		return t, status, err
	}
	// if sp.From.Source != nil && len(sp.From.Source.With) > 0 && sp.From.Source.With.Bool("distributed") {
	// 	u.Warnf("has distributed!!!!!: %#v", sp.From.Source.With)
	// }
	return t, status, err
}
