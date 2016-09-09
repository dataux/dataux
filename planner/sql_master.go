package planner

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"time"

	u "github.com/araddon/gou"
	"github.com/lytics/grid"
	"github.com/lytics/grid/condition"
	"github.com/lytics/grid/ring"

	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/plan"
)

// sql task, the master process to run the child actors
type sqlMasterTask struct {
	s              *PlannerGrid
	p              *plan.Select
	ns             *SourceNats
	flow           Flow
	completionTask exec.TaskRunner
	actorCt        int
	done           chan bool
}

func newSqlMasterTask(s *PlannerGrid, completionTask exec.TaskRunner,
	ns *SourceNats, flow Flow, p *plan.Select) *sqlMasterTask {

	return &sqlMasterTask{
		s:              s,
		p:              p,
		flow:           flow,
		completionTask: completionTask,
		ns:             ns,
		done:           make(chan bool),
	}
}
func (m *sqlMasterTask) startSqlActor(actorId int, partition, pb string, def *grid.ActorDef) error {

	def.DefineType("sqlactor")
	def.Define("flow", m.flow.Name())
	def.Settings["pb64"] = pb
	def.Settings["partition"] = partition
	def.Settings["actor_ct"] = strconv.Itoa(m.actorCt)
	u.Debugf("%p submitting start actor %s  nodeI=%d", m, def.ID(), actorId)
	err := m.s.Grid.StartActor(def)
	if err != nil {
		u.Errorf("error: failed to start: %v, due to: %v", "sqlactor", err)
	}
	return err
}

func (m *sqlMasterTask) heartbeat(rp ring.Ring) {
	// observe our ring of child actors, they are responsible for checking in every xx seconds
	// to tell us they are alive, if they don't we cancel those tasks?
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-m.done:
			u.Warnf("m.done???")
			return
		case <-ticker.C:
			u.Debugf("alive")
		case msg, ok := <-m.ns.cmdch:
			if !ok {
				u.Debugf("cmd channel closed")
				return
			}
			u.Infof("%p got cmd msg %+v", msg)
		}
	}
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
	} else {
		u.Warnf("TODO:  NOT Distributed, don't start tasks!")
	}

	_, err = m.s.Grid.Etcd().CreateDir(fmt.Sprintf("/%v/%v/%v", m.s.Grid.Name(), m.flow.Name(), "sqlcomplete"), 100000)
	if err != nil {
		u.Errorf("Could not initilize dir %v", err)
	}
	_, err = m.s.Grid.Etcd().CreateDir(fmt.Sprintf("/%v/%v/%v", m.s.Grid.Name(), m.flow.Name(), "sql_master_done"), 100000)
	if err != nil {
		u.Errorf("Could not initilize dir %v", err)
	}
	_, err = m.s.Grid.Etcd().CreateDir(fmt.Sprintf("/%v/%v/%v", m.s.Grid.Name(), m.flow.Name(), "finished"), 100000)
	if err != nil {
		u.Errorf("Could not initilize dir %v", err)
	}

	w := condition.NewCountWatch(m.s.Grid.Etcd(), m.s.Grid.Name(), m.flow.Name(), "finished")
	defer w.Stop()

	finished := w.WatchUntil(m.actorCt)

	rp := ring.New(m.flow.NewContextualName("sqlactor"), m.actorCt)
	u.Debugf("%p master?? submitting distributed sql query with %d actors", m, m.actorCt)
	for i, def := range rp.ActorDefs() {
		go func(ad *grid.ActorDef, actorId int) {
			if err = m.startSqlActor(actorId, partitions[actorId], pb64, ad); err != nil {
				u.Errorf("Could not create sql actor %v", err)
			}
		}(def, i)
	}

	// make sure actors are alive and
	// also watch for cmd messages
	go m.heartbeat(rp)

	//u.Debugf("submitted actors, waiting for completion signal")
	sendComplete := func() {
		u.Debugf("CompletionTask finished sending shutdown signal %s/%s/%s ", m.s.Grid.Name(), m.flow.Name(), "sql_master_done")
		jdone := condition.NewJoin(m.s.Grid.Etcd(), 10*time.Second, m.s.Grid.Name(), m.flow.Name(), "sql_master_done", "master")
		if err = jdone.Rejoin(); err != nil {
			u.Errorf("could not join?? %v", err)
		}
		time.Sleep(time.Millisecond * 50)
		defer jdone.Stop()
	}
	select {
	case <-finished:
		u.Infof("%s got all finished signal?", m.flow.Name())
		return nil
	case <-m.completionTask.SigChan():
		sendComplete()
		//case <-time.After(30 * time.Second):
		//	u.Warnf("%s exiting bc timeout", flow)
	}
	return nil
}
