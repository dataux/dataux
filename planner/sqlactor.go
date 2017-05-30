package planner

import (
	"context"
	"fmt"
	"time"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/plan"
	"github.com/lytics/dfa"
	"github.com/lytics/grid/grid.v3"
)

type (
	// SqlActor a worker/actor that runs in distributed grid nodes
	// and receives messages from the planner to fulfill parts of
	// sql dag of tasks in order to complete a query.
	SqlActor struct {
		name   string
		id     string
		ctx    context.Context
		conf   *Conf
		client *grid.Client
		server *grid.Server
		mbox   *grid.Mailbox
		exit   <-chan bool
	}

	sqlActorTask struct {
		p           *plan.Select
		et          exec.TaskRunner
		sqlJobMaker interface{}
		ActorCt     int
	}
)

// LeaderCreate factory function to create the Leader
func WorkerFactory(conf *Conf, client *grid.Client, server *grid.Server) grid.MakeActor {
	return func(actorConf []byte) (grid.Actor, error) {
		//u.Debugf("worker create %s", string(actorConf))
		sa := &SqlActor{
			server: server,
			client: client,
			conf:   conf,
		}
		return sa, nil
	}
}

func (m *SqlActor) ID() string {
	return m.id
}

func (m *SqlActor) String() string {
	return m.name
}

func (m *SqlActor) Act(ctx context.Context) {

	m.ctx = ctx

	d := dfa.New()
	d.SetStartState(Starting)
	d.SetTerminalStates(Exiting)
	d.SetTransitionLogger(func(state dfa.State) {
		//u.Infof("%v: switched to state: %v", m, state)
	})

	d.SetTransition(Starting, Started, Running, m.Running)
	d.SetTransition(Starting, Failure, Exiting, m.Exiting)
	d.SetTransition(Starting, Exit, Exiting, m.Exiting)

	d.SetTransition(Running, Finished, Exiting, m.Exiting)
	d.SetTransition(Running, Failure, Exiting, m.Exiting)
	d.SetTransition(Running, Exit, Exiting, m.Exiting)

	final, _ := d.Run(m.Starting)

	u.Infof("%s sqlworker error : %v", m, final.String())
}

func (m *SqlActor) Starting() dfa.Letter {
	//u.Debugf("%p Starting", m)

	return Started
}

func (m *SqlActor) runTask(t *SqlTask) error {

	//u.Debugf("m.conf %#v", m.conf)
	//u.Debugf("t %#v", t)
	p, err := plan.SelectPlanFromPbBytes(t.Pb, m.conf.SchemaLoader)
	if err != nil {
		u.Errorf("error %v", err)
		return err
	}

	if p.ChildDag == false {
		u.Errorf("%p This MUST BE CHILD DAG", p)
	}

	if m.conf == nil {
		u.Warnf("no conf?")
		return fmt.Errorf("no conf")
	}

	if m.conf.JobMaker == nil {
		u.Warnf("no JobMaker?")
		return fmt.Errorf("no job maker")
	}

	executor, err := m.conf.JobMaker(p.Ctx)
	if err != nil {
		u.Errorf("error on job maker%v", err)
		return err
	}

	u.Debugf("Exec Plan %s", p.Stmt)

	//u.Debugf("nodeCt:%v  run executor walk select %#v from ct? %v", nodeCt, p.Stmt.With, len(p.From))
	for _, f := range p.From {
		if len(f.Custom) == 0 {
			f.Custom = make(u.JsonHelper)
		}
		f.Custom["partition"] = t.Partition
		//u.Infof("actor from custom has part?: %#v", f.Custom)
	}

	//u.Infof("%p plan.Select:%p sqlactor calling executor %p", m, p, executor)
	sqlTask, err := executor.WalkSelectPartition(p, nil)
	//sqlTask, err := executor.WalkPlan(p)
	if err != nil {
		u.Errorf("Could not create select task %v", err)
		return err
	}
	tr, ok := sqlTask.(exec.TaskRunner)
	if !ok {
		u.Errorf("Expected exec.TaskRunner but got %T", sqlTask)
		return fmt.Errorf("task was not TaskRunner")
	}

	//u.Debugf("%p running with %d nodes %+v", m, t.ActorCount, t)

	// Now run the sql dag exec tasks
	go func() {
		send := func(msg interface{}) (interface{}, error) {
			return m.client.Request(timeout, t.Source, msg)
		}
		sink := NewSink(p.Ctx, t.Source, send)
		tr.Add(sink)
		tr.Setup(0) // Setup our Task in the DAG

		err := tr.Run()
		u.Debugf("%p finished sqldag %s", m, m.ID())
		if err != nil {
			u.Errorf("error on Query.Run(): %v", err)
		}
	}()
	return nil
}

func (m *SqlActor) Running() dfa.Letter {

	m.name, _ = grid.ContextActorName(m.ctx)
	m.id, _ = grid.ContextActorID(m.ctx)

	u.Infof("Running %q id=%q", m.name, m.id)

	// Listen to a mailbox with the same
	// name as the actor.
	mailbox, err := grid.NewMailbox(m.server, m.name, 10)
	if err != nil {
		u.Errorf("could not create mailbox %v", err)
		return Failure
	}
	m.mbox = mailbox
	defer mailbox.Close()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			u.Warnf("ctx.Done()")
			return Exit
		case <-m.exit:
			u.Debugf("%s exit?   what caused this?", m)
			return Exit
		case req := <-mailbox.C:

			// Like any actor we can recieve normal messages
			//u.Debugf("%s mbox Request:  %#v", m, req)
			switch task := req.Msg().(type) {
			case *SqlTask:
				//u.Infof("sqltask %+v\n", task)
				err := req.Respond(&TaskResponse{Id: m.name})
				if err != nil {
					u.Errorf("error on message response %v\n", err)
					continue
				}
				go m.runTask(task)
			default:
				u.Errorf("ERROR:  wrong type %T", task)
			}
		}
	}
}

func (m *SqlActor) Finishing() dfa.Letter {
	u.Debugf("%s sqlactor finishing", m.String())
	return Exit
}

func (m *SqlActor) Exiting() {
	u.Debugf("exiting")
}
