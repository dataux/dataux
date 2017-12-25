package planner

import (
	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/plan"
	"github.com/lytics/dfa"
)

var (
	// DFA States
	Starting = dfa.State("starting")
	Running  = dfa.State("running")
	Exiting  = dfa.State("exiting")
	// Letters
	Failure  = dfa.Letter("failure")
	Started  = dfa.Letter("started")
	Finished = dfa.Letter("finished")
	Exit     = dfa.Letter("exit")
)

type JobMaker func(ctx *plan.Context) (*GridTask, error)

type Conf struct {
	JobMaker       JobMaker
	SchemaLoader   plan.SchemaLoader
	SupressRecover bool
	NodeCt         int
	MailboxCount   int
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
		u.Warnf("no schema loader?")
	}
	return &Conf{
		JobMaker:       c.JobMaker,
		SchemaLoader:   c.SchemaLoader,
		SupressRecover: c.SupressRecover,
		NodeCt:         c.NodeCt,
		MailboxCount:   c.MailboxCount,
		Address:        c.Address,
		GridName:       c.GridName,
		Hostname:       c.Hostname,
		EtcdServers:    c.EtcdServers,
		NatsServers:    c.NatsServers,
	}
}
