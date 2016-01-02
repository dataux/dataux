package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	u "github.com/araddon/gou"
	"github.com/lytics/grid/grid2"
	"github.com/lytics/grid/grid2/condition"
	"github.com/lytics/grid/grid2/ring"
	"github.com/lytics/metafora"
)

var (
	etcdconnect = flag.String("etcd", "http://127.0.0.1:2379", "comma separated list of etcd urls to servers")
	natsconnect = flag.String("nats", "nats://127.0.0.1:4222", "comma separated list of nats urls to servers")
	producers   = flag.Int("producers", 3, "number of producers")
	consumers   = flag.Int("consumers", 2, "number of consumers")
	nodes       = flag.Int("nodes", 1, "number of nodes in the grid")
	nodeName    = flag.String("nodename", "", "name of host, override localhost for multi-nodes on same machine")
	msgsize     = flag.Int("msgsize", 1000, "minimum message size, actual size will be in the range [min,2*min]")
	msgcount    = flag.Int("msgcount", 100000, "number of messages each producer should send")
)

type server struct {
	conf    *Conf
	g       grid2.Grid
	gm      grid2.Grid
	started bool
	taskId  int
}

func (s *server) runFlow() interface{} {
	var err error
	flow := NewFlow(s.taskId)
	s.taskId++

	u.Debugf("%s starting job ", flow)

	// Attempting to find out when this task has finished
	// and want to go read Store() for getting state?
	f := condition.NewNameWatch(s.g.Etcd(), s.g.Name(), flow.Name(), "finished")
	finished := f.WatchUntil(flow.NewContextualName("leader"))

	rp := ring.New(flow.NewContextualName("producer"), s.conf.NrProducers)
	for _, def := range rp.ActorDefs() {
		def.DefineType("producer")
		def.Define("flow", flow.Name())
		err := s.gm.StartActor(def)
		if err != nil {
			u.Errorf("error: failed to start: %v, due to: %v", def, err)
			os.Exit(1)
		}
	}

	rc := ring.New(flow.NewContextualName("consumer"), s.conf.NrConsumers)
	for _, def := range rc.ActorDefs() {
		def.DefineType("consumer")
		def.Define("flow", flow.Name())
		err := s.gm.StartActor(def)
		if err != nil {
			u.Errorf("error: failed to start: %v, due to: %v", def, err)
			os.Exit(1)
		}
	}

	ldr := grid2.NewActorDef(flow.NewContextualName("leader"))
	ldr.DefineType("leader")
	ldr.Define("flow", flow.Name())
	err = s.gm.StartActor(ldr)
	if err != nil {
		u.Errorf("error: failed to start: %v, due to: %v", "leader", err)
		os.Exit(1)
	}

	st := condition.NewState(s.g.Etcd(), 10*time.Minute, s.g.Name(), flow.Name(), "state", ldr.ID())
	defer st.Stop()

	// can we read the reader state?

	select {
	case <-finished:
		u.Warnf("%s YAAAAAY finished", flow.String())
		var state LeaderState
		if _, err := st.Fetch(&state); err != nil {
			u.Warnf("%s failed to read state: %v", flow, err)
		} else {
			u.Infof("%s got STATE: %#v", flow, state)
			return state
		}
	case <-time.After(30 * time.Second):
		u.Warnf("%s exiting bc timeout", flow)
	}
	return nil
}

func (s *server) taskHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if s.started {
		state := s.runFlow()
		if state != nil {
			by, _ := json.Marshal(map[string]interface{}{"status": "success", "state": state})
			fmt.Fprintf(w, string(by))
		} else {
			fmt.Fprintf(w, `{"status":"failure","message":"no restult"}`)
		}
	} else {
		fmt.Fprintf(w, `{"status":"failure","message":"not started yet"}`)
	}
}

func (s *server) Run() {

	m, err := newActorMaker(s.conf)
	if err != nil {
		u.Errorf("error: failed to make actor maker: %v", err)
	}

	// We are going to start a "Grid Master" for starting tasks
	// but it won't do any work
	s.gm = grid2.NewGridDetails(s.conf.GridName, s.conf.Hostname+"Master", s.conf.EtcdServers, s.conf.NatsServers, &nilMaker{})
	exit, err := s.gm.Start()
	if err != nil {
		u.Errorf("error: failed to start grid master: %v", err)
		os.Exit(1)
	}

	s.g = grid2.NewGridDetails(s.conf.GridName, s.conf.Hostname, s.conf.EtcdServers, s.conf.NatsServers, m)

	exit, err = s.g.Start()
	if err != nil {
		u.Errorf("error: failed to start grid: %v", err)
		os.Exit(1)
	}

	j := condition.NewJoin(s.g.Etcd(), 30*time.Second, s.g.Name(), "hosts", s.conf.Hostname)
	err = j.Join()
	if err != nil {
		u.Errorf("error: failed to regester: %v", err)
		os.Exit(1)
	}
	defer j.Exit()
	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-exit:
				return
			case <-ticker.C:
				err := j.Alive()
				if err != nil {
					u.Errorf("error: failed to report liveness: %v", err)
					os.Exit(1)
				}
			}
		}
	}()

	w := condition.NewCountWatch(s.g.Etcd(), s.g.Name(), "hosts")
	defer w.Stop()

	u.Debugf("waiting for %d nodes to join", *nodes)
	started := w.WatchUntil(*nodes)
	select {
	case <-exit:
		u.Debug("Shutting down, grid exited")
		return
	case <-w.WatchError():
		u.Errorf("error: failed to watch other hosts join: %v", err)
		os.Exit(1)
	case <-started:
		s.started = true
		u.Infof("now started")
	}

	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
		select {
		case <-sig:
			u.Debug("shutting down")
			s.g.Stop()
		case <-exit:
		}
	}()

	<-exit
	u.Info("shutdown complete")
}
func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	flag.Parse()

	u.SetLogger(log.New(os.Stderr, "", log.Ltime|log.Lmicroseconds|log.Lshortfile), "debug")
	u.SetColorOutput()
	metafora.SetLogger(u.GetLogger()) // Configure metafora's logger
	metafora.SetLogLevel(metafora.LogLevelWarn)
	u.DiscardStandardLogger() // Discard non-sanctioned spammers

	etcdservers := strings.Split(*etcdconnect, ",")
	natsservers := strings.Split(*natsconnect, ",")

	hostname, err := os.Hostname()
	if err != nil {
		u.Errorf("error: failed to discover hostname: %v", err)
		os.Exit(1)
	}
	if *nodeName != "" {
		hostname = *nodeName
	}

	conf := &Conf{
		GridName:    "dataux",
		Hostname:    hostname,
		MsgSize:     *msgsize,
		MsgCount:    *msgcount,
		NrProducers: *producers,
		NrConsumers: *consumers,
		EtcdServers: etcdservers,
		NatsServers: natsservers,
	}

	s := &server{conf: conf}

	http.HandleFunc("/task", s.taskHandler)

	go func() {
		u.Warn(http.ListenAndServe("localhost:6060", nil))
	}()

	s.Run() // blocking
}

type Flow string

func NewFlow(nr int) Flow {
	return Flow(fmt.Sprintf("flow-%v", nr))
}

func (f Flow) NewContextualName(name string) string {
	return fmt.Sprintf("%v-%v", f, name)
}

func (f Flow) Name() string {
	return string(f)
}

func (f Flow) String() string {
	return string(f)
}
