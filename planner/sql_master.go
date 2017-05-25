package planner

import (
	"encoding/base64"
	"fmt"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/plan"
	"github.com/lytics/grid/grid.v3"
	"github.com/lytics/grid/grid.v3/ring"
)

// sql task, the master process to run the child actors
type sqlMasterTask struct {
	s              *PlannerGrid
	p              *plan.Select
	ns             *Source
	flow           Flow
	completionTask exec.TaskRunner
	actorCt        int
	done           chan bool
}

func newSqlMasterTask(s *PlannerGrid,
	completionTask exec.TaskRunner,
	ns *Source,
	flow Flow,
	p *plan.Select) *sqlMasterTask {

	return &sqlMasterTask{
		s:              s,
		p:              p,
		flow:           flow,
		completionTask: completionTask,
		ns:             ns,
		done:           make(chan bool),
	}
}
func (m *sqlMasterTask) startSqlTask(a *grid.ActorStart, partition, pb string, pbb []byte) error {

	t := SqlTask{}
	t.Id = fmt.Sprintf("sql-%v", NextIdUnsafe())
	t.Pb = pbb
	t.Partition = partition
	t.ActorCount = int32(m.actorCt)
	u.Debugf("%p submitting start actor %s  nodeI=%d", m, a.GetName())
	_, err := m.s.gridClient.Request(timeout, "sqlworker-1", &t)
	if err != nil {
		u.Errorf("error: failed to start: %v, due to: %v", "sqlactor", err)
	}
	return err
}

// Submits a Sql Select statement task for planning across multiple nodes
func (m *sqlMasterTask) Run() error {

	defer func() {
		close(m.done)
	}()
	//u.Debugf("%p master submitting job childdag?%v  %s", m, p.ChildDag, p.Stmt.String())
	// marshal plan to Protobuf for transport
	pb, err := m.p.Marshal()
	if err != nil {
		u.Errorf("Could not protbuf marshal %v for %s", err, m.p.Stmt)
		return err
	}
	// TODO:  send the instructions as a grid message NOT part of actor-def
	pb64 := base64.URLEncoding.EncodeToString(pb)
	//u.Infof("pb64:  %s", pb64)

	m.actorCt = 1
	partitions := []string{""}
	if len(m.p.Stmt.With) > 0 && m.p.Stmt.With.Bool("distributed") {
		//u.Warnf("distribution instructions node_ct:%v", p.Stmt.With.Int("node_ct"))
		for _, f := range m.p.From {
			if f.Tbl != nil {
				if f.Tbl.Partition != nil {
					partitions = make([]string, len(f.Tbl.Partition.Partitions))
					m.actorCt = len(f.Tbl.Partition.Partitions)
					for i, part := range f.Tbl.Partition.Partitions {
						//u.Warnf("Found Partitions for %q = %#v", f.Tbl.Name, part)
						partitions[i] = part.Id
					}
				} else if f.Tbl.PartitionCt > 0 {
					partitions = make([]string, f.Tbl.PartitionCt)
					m.actorCt = f.Tbl.PartitionCt
					for i := 0; i < m.actorCt; i++ {
						//u.Warnf("Found Partitions for %q = %#v", f.Tbl.Name, i)
						partitions[i] = fmt.Sprintf("%d", i)
					}
				} else {
					u.Warnf("partition? %#v", f.Tbl.Partition)
				}
			}
		}

		rp := ring.New(m.flow.NewContextualName("sqlworker"), m.actorCt)
		u.Debugf("%p master?? submitting distributed sql query with %d actors", m, m.actorCt)
		for i, def := range rp.Actors() {
			go func(ad *grid.ActorStart, actorId int) {
				if err = m.startSqlTask(ad, partitions[actorId], pb64, pb); err != nil {
					u.Errorf("Could not create sql actor %v", err)
				}
			}(def, i)
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
