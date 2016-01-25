package gridrunner

import (
	"encoding/gob"

	"github.com/lytics/dfa"
)

var (
	// States
	Starting    = dfa.State("starting")
	Running     = dfa.State("running")
	Resending   = dfa.State("resending")
	Finishing   = dfa.State("finishing")
	Exiting     = dfa.State("exiting")
	Terminating = dfa.State("terminating")
	// Letters
	Failure            = dfa.Letter("failure")
	SendFailure        = dfa.Letter("send-failure")
	SendSuccess        = dfa.Letter("send-success")
	FetchStateFailure  = dfa.Letter("fetch-state-failure")
	StoreStateFailure  = dfa.Letter("store-state-failure")
	EverybodyStarted   = dfa.Letter("everybody-started")
	EverybodyFinished  = dfa.Letter("everybody-finished")
	IndividualFinished = dfa.Letter("individual-finished")
	Exit               = dfa.Letter("exit")
)

func init() {
	gob.Register(ResultMsg{})
	gob.Register(DataMsg{})
}

type Conf struct {
	NodeCt      int
	GridName    string
	Hostname    string
	MsgSize     int
	MsgCount    int
	NrProducers int
	NrConsumers int
	EtcdServers []string
	NatsServers []string
}

func (c *Conf) Clone() *Conf {
	return &Conf{
		NodeCt:      c.NodeCt,
		GridName:    c.GridName,
		Hostname:    c.Hostname,
		MsgSize:     c.MsgSize,
		MsgCount:    c.MsgCount,
		NrProducers: c.NrProducers,
		NrConsumers: c.NrConsumers,
		EtcdServers: c.EtcdServers,
		NatsServers: c.NatsServers,
	}
}

type DataMsg struct {
	Producer string
	Data     string
}

type ResultMsg struct {
	Producer string
	From     string
	Count    int
	Duration float64
}
