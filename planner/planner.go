package planner

import (
	"fmt"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/plan"
)

var (
	_ = u.EMPTY

	// ensure we implement interface
	_ plan.TaskPlanner = (*TaskRunners)(nil)
)

/*
const (
	ItemDefaultChannelSize = 50
)

type SigChan chan bool
type ErrChan chan error
type MessageChan chan schema.Message

// Handle/Forward a message for this Task
type MessageHandler func(ctx *plan.Context, msg schema.Message) bool

// TaskRunner is an interface for a single task in Dag of Tasks necessary to execute a Job
// - it may have children tasks
// - it may be parallel, distributed, etc
type TaskRunner interface {
	plan.Task
	Type() string
	Setup(depth int) error
	MessageIn() MessageChan
	MessageOut() MessageChan
	MessageInSet(MessageChan)
	MessageOutSet(MessageChan)
	ErrChan() ErrChan
	SigChan() SigChan
}
*/

type TaskRunners struct {
	ctx     *plan.Context
	tasks   []plan.Task
	runners []exec.TaskRunner
}

func TaskRunnersMaker(ctx *plan.Context) plan.TaskPlanner {
	return &TaskRunners{
		ctx:     ctx,
		tasks:   make([]plan.Task, 0),
		runners: make([]exec.TaskRunner, 0),
	}
}

func (m *TaskRunners) Close() error          { return nil }
func (m *TaskRunners) Run() error            { return nil }
func (m *TaskRunners) Children() []plan.Task { return m.tasks }
func (m *TaskRunners) Add(task plan.Task) error {
	tr, ok := task.(exec.TaskRunner)
	u.Debugf("dataux.Planner")
	if !ok {
		panic(fmt.Sprintf("must be taskrunner %T", task))
	}
	m.tasks = append(m.tasks, task)
	m.runners = append(m.runners, tr)
	return nil
}
func (m *TaskRunners) Sequential(name string) plan.Task {
	return exec.NewSequential(m.ctx, name, m)
}
func (m *TaskRunners) Parallel(name string, tasks []plan.Task) plan.Task {
	return exec.NewTaskParallel(m.ctx, name, tasks)
}
