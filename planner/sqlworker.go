package planner

import (
	"fmt"
	"os"
	"time"

	u "github.com/araddon/gou"
	"github.com/lytics/grid/grid.v2"
	"github.com/lytics/grid/grid.v2/condition"

	"github.com/araddon/qlbridge/datasource"
)

func RunWorkerNodes(quit chan bool, nodeCt int, r *datasource.Registry) {

	loggingOnce.Do(setupLogging)
	nextId, _ := NextId()

	for i := 0; i < nodeCt; i++ {
		go func(nodeId int) {
			s := NewTaskServer(nodeCt, r)
			s.Conf.Hostname = NodeName2(nextId, uint64(nodeId))
			err := s.Run(quit) // blocking
			if err != nil {
				u.Warnf("could not start worker")
			}
		}(i)
	}
	time.Sleep(time.Millisecond * 80)
}

// TaskServer accepts and performs
type TaskServer struct {
	Conf       *Conf
	reg        *datasource.Registry
	maker      grid.ActorMaker
	Grid       grid.Grid
	started    bool
	lastTaskId uint64
}

func NewTaskServer(nodeCt int, r *datasource.Registry) *TaskServer {
	nextId, _ := NextId()

	conf := GridConf.Clone()
	maker, err := newActorMaker(conf)
	if err != nil {
		panic(fmt.Errorf("could not start worker %v", err))
	}
	conf.NodeCt = nodeCt
	conf.Hostname = NodeName(nextId)
	return &TaskServer{Conf: conf, reg: r, maker: maker}
}

func (s *TaskServer) Run(quit chan bool) error {

	// We are going to start a "Grid" with specified maker
	//   - nilMaker = "master" only used for submitting tasks, not performing them
	//   - normal maker;  performs specified work units
	s.Grid = grid.New(s.Conf.GridName, s.Conf.Hostname, s.Conf.EtcdServers, s.Conf.NatsServers, s.maker)

	//u.Debugf("%p created new distributed grid sql job maker: %#v", s, s.Grid)
	exit, err := s.Grid.Start()
	if err != nil {
		u.Errorf("failed to start grid: %v", err)
		return fmt.Errorf("error starting grid %v", err)
	}

	defer func() {
		u.Debugf("defer grid worker complete: %s", s.Conf.Hostname)
		s.Grid.Stop()
	}()

	complete := make(chan bool)

	j := condition.NewJoin(s.Grid.Etcd(), 30*time.Second, s.Grid.Name(), "hosts", s.Conf.Hostname)
	err = j.Join()
	if err != nil {
		u.Errorf("failed to register grid node: %v", err)
		os.Exit(1)
	}
	defer j.Exit()
	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-quit:
				//u.Debugf("quit signal")
				close(complete)
				return
			case <-exit:
				//u.Debugf("worker grid exit??")
				return
			case <-ticker.C:
				err := j.Alive()
				if err != nil {
					u.Errorf("failed to report liveness: %v", err)
					os.Exit(1)
				}
			}
		}
	}()

	w := condition.NewCountWatch(s.Grid.Etcd(), s.Grid.Name(), "hosts")
	defer w.Stop()

	waitForCt := s.Conf.NodeCt + 1 // worker nodes + master
	//u.Debugf("%p waiting for %d nodes to join", s, waitForCt)
	//u.LogTraceDf(u.WARN, 16, "")
	started := w.WatchUntil(waitForCt)
	select {
	case <-complete:
		u.Debugf("got complete signal")
		return nil
	case <-exit:
		//u.Debug("Shutting down, grid exited")
		return nil
	case <-w.WatchError():
		u.Errorf("failed to watch other hosts join: %v", err)
		os.Exit(1)
	case <-started:
		s.started = true
		//u.Debugf("%p now started", s)
	}
	<-exit
	//u.Debug("shutdown complete")
	return nil
}
