package gridrunner

import (
	"encoding/gob"
	"time"

	u "github.com/araddon/gou"

	"github.com/lytics/dfa"
	"github.com/lytics/grid/grid2"
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

type Chaos struct {
	roll chan bool
	stop chan bool
	C    <-chan bool
}

func NewChaos(name string) *Chaos {
	stop := make(chan bool)
	roll := make(chan bool, 1)
	go func() {
		dice := grid2.NewSeededRand()
		delay := time.Duration(30+dice.Intn(600)) * time.Second
		ticker := time.NewTicker(delay)
		happen := ticker.C
		for {
			select {
			case <-stop:
				ticker.Stop()
				return
			case <-happen:
				ticker.Stop()
				delay := time.Duration(30+dice.Intn(600)) * time.Second
				u.Debugf("%v: CHAOS", name)
				ticker = time.NewTicker(delay)
				happen = ticker.C
				select {
				case <-stop:
					ticker.Stop()
				case roll <- true:
				}
			}
		}
	}()
	return &Chaos{stop: stop, roll: roll, C: roll}
}

func (c *Chaos) Stop() {
	close(c.stop)
}
