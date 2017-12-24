package planner

import (
	"fmt"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/plan"
)

// sql task, the master process to run the child actors
type sqlMasterTask struct {
	s              *PlannerGrid
	p              *plan.Select
	ns             *Source
	completionTask exec.TaskRunner
	pbb            []byte
	actorCt        int
	distributed    bool
	done           chan bool
	partitions     []string
	workersIds     []int
}

func newSqlMasterTask(s *PlannerGrid,
	completionTask exec.TaskRunner,
	ns *Source,
	p *plan.Select) *sqlMasterTask {

	return &sqlMasterTask{
		s:              s,
		p:              p,
		completionTask: completionTask,
		ns:             ns,
		done:           make(chan bool),
	}
}
func (m *sqlMasterTask) startSqlTask(partition string, workerId int) error {

	mailbox := fmt.Sprintf("sqlworker-%d", workerId)

	t := SqlTask{}
	t.Id = fmt.Sprintf("sql-%v", NextIdUnsafe()) // The mailbox ID we are listening on
	t.Pb = m.pbb
	t.Source = m.ns.MailboxId()
	t.Partition = partition
	t.ActorCount = int32(m.actorCt)

	u.Debugf("%p submitting start task actor worker=%s", m, mailbox)

	// this is going to send the Task to a sqlworker to run
	_, err := m.s.gridClient.Request(timeout, mailbox, &t)
	if err != nil {
		u.Errorf("error: failed to start: %v, due to: %v", "sqlactor", err)
	}
	return err
}

func (m *sqlMasterTask) init() error {

	//u.Debugf("%p master submitting job childdag?%v  %s", m, p.ChildDag, p.Stmt.String())
	// marshal plan to Protobuf for transport
	pb, err := m.p.Marshal()
	if err != nil {
		u.Errorf("Could not protbuf marshal %v for %s", err, m.p.Stmt)
		return err
	}
	m.pbb = pb
	// TODO:  send the instructions as a grid message NOT part of actor-def
	//m.pb64 = base64.URLEncoding.EncodeToString(pb)
	//u.Infof("pb64:  %s", pb64)

	m.actorCt = 1
	m.partitions = []string{""}
	if len(m.p.Stmt.With) > 0 && m.p.Stmt.With.Bool("distributed") {
		m.distributed = true
		//u.Warnf("distribution instructions node_ct:%v", p.Stmt.With.Int("node_ct"))
		for _, f := range m.p.From {
			if f.Tbl != nil {
				if f.Tbl.Partition != nil {
					m.partitions = make([]string, len(f.Tbl.Partition.Partitions))
					m.actorCt = len(f.Tbl.Partition.Partitions)
					for i, part := range f.Tbl.Partition.Partitions {
						//u.Warnf("Found Partitions for %q = %#v", f.Tbl.Name, part)
						m.partitions[i] = part.Id
					}
				} else if f.Tbl.PartitionCt > 0 {
					m.partitions = make([]string, f.Tbl.PartitionCt)
					m.actorCt = int(f.Tbl.PartitionCt)
					for i := 0; i < m.actorCt; i++ {
						//u.Warnf("Found Partitions for %q = %#v", f.Tbl.Name, i)
						m.partitions[i] = fmt.Sprintf("%d", i)
					}
				} else {
					u.Warnf("partition? %#v", f.Tbl.Partition)
				}
			}
		}

		m.ns.sinkCt = m.actorCt
		m.workersIds = m.s.peers.GetPeers(m.actorCt)
		u.Infof("About to Run, has source sinkCt=%v workers=%v", m.ns.sinkCt, m.workersIds)

	} else {
		u.Warnf("TODO:  NOT Distributed, don't start tasks!")
	}

	return nil
}

// Submits a Sql Select statement task for planning across multiple nodes
func (m *sqlMasterTask) Run() error {

	defer func() {
		close(m.done)
	}()
	//u.Debugf("%p master submitting job childdag?%v  %s", m, p.ChildDag, p.Stmt.String())

	if m.distributed {
		for i, worker := range m.workersIds {
			if err := m.startSqlTask(m.partitions[i], worker); err != nil {
				u.Errorf("Could not create sql actor %v", err)
			}
		}

	} else {
		u.Warnf("TODO:  NOT Distributed, don't start tasks!")
	}

	select {
	case <-m.completionTask.SigChan():
		u.Debugf("completion")
	}
	return nil
}
