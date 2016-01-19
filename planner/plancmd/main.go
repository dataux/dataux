package main

import (
	"flag"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"strings"

	u "github.com/araddon/gou"

	"github.com/lytics/metafora"

	"github.com/dataux/dataux/planner/gridrunner"
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

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	flag.Parse()

	u.SetLogger(log.New(os.Stderr, "", log.Ltime|log.Lmicroseconds|log.Lshortfile), "debug")
	u.SetColorOutput()
	metafora.SetLogger(u.GetLogger()) // Configure metafora's logger
	metafora.SetLogLevel(metafora.LogLevelWarn)
	u.DiscardStandardLogger() // Discard non-sanctioned spammers

	conf := &gridrunner.Conf{
		NodeCt:      *nodes,
		GridName:    "dataux",
		Hostname:    nodeId(),
		MsgSize:     *msgsize,
		MsgCount:    *msgcount,
		NrProducers: *producers,
		NrConsumers: *consumers,
		EtcdServers: strings.Split(*etcdconnect, ","),
		NatsServers: strings.Split(*natsconnect, ","),
	}

	s := &gridrunner.Server{Conf: conf}

	http.HandleFunc("/task", s.TaskHandler)

	go func() {
		u.Warn(http.ListenAndServe("localhost:6060", nil))
	}()

	s.Run() // blocking
}

func nodeId() string {
	hostname, err := os.Hostname()
	if err != nil {
		u.Errorf("error: failed to discover hostname: %v", err)
		os.Exit(1)
	}
	if *nodeName != "" {
		hostname = *nodeName
	}
	return hostname
}
