package planner

import (
	"fmt"

	"github.com/lytics/dfa"

	"github.com/araddon/qlbridge/plan"
)

var (
	// States
	Starting = dfa.State("starting")
	Running  = dfa.State("running")
	Exiting  = dfa.State("exiting")
	// Letters
	Failure  = dfa.Letter("failure")
	Started  = dfa.Letter("started")
	Finished = dfa.Letter("finished")
	Exit     = dfa.Letter("exit")
)

type JobMaker func(ctx *plan.Context) (*ExecutorGrid, error)

type Flow string

func NewFlow(nr uint64) Flow {
	return Flow(fmt.Sprintf("sql-%v", nr))
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

type Conf struct {
	JobMaker       JobMaker
	SchemaLoader   plan.SchemaLoader
	SupressRecover bool
	NodeCt         int
	Address        string
	GridName       string
	Hostname       string
	EtcdServers    []string
	NatsServers    []string
}

func (c *Conf) Clone() *Conf {
	if c.JobMaker == nil {
		//panic("need job maker")
	}
	if c.SchemaLoader == nil {
		//panic("need SchemaLoader")
	}
	return &Conf{
		JobMaker:       c.JobMaker,
		SchemaLoader:   c.SchemaLoader,
		SupressRecover: c.SupressRecover,
		NodeCt:         c.NodeCt,
		Address:        c.Address,
		GridName:       c.GridName,
		Hostname:       c.Hostname,
		EtcdServers:    c.EtcdServers,
		NatsServers:    c.NatsServers,
	}
}
