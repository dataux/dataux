package proxy

import (
	"github.com/dataux/dataux/vendored/mixer/client"
	"github.com/dataux/dataux/vendored/mixer/mysql"
)

func (c *Conn) isInTransaction() bool {
	return c.Status&mysql.SERVER_STATUS_IN_TRANS > 0
}

func (c *Conn) isAutoCommit() bool {
	return c.Status&mysql.SERVER_STATUS_AUTOCOMMIT > 0
}

func (c *Conn) handleBegin() error {
	c.Status |= mysql.SERVER_STATUS_IN_TRANS
	return c.WriteOK(nil)
}

func (c *Conn) handleCommit() (err error) {
	if err := c.commit(); err != nil {
		return err
	} else {
		return c.WriteOK(nil)
	}
}

func (c *Conn) handleRollback() (err error) {
	if err := c.rollback(); err != nil {
		return err
	} else {
		return c.WriteOK(nil)
	}
}

func (c *Conn) commit() (err error) {
	c.Status &= ^mysql.SERVER_STATUS_IN_TRANS

	for _, co := range c.txConns {
		if e := co.Commit(); e != nil {
			err = e
		}
		co.Close()
	}

	c.txConns = map[*Node]*client.SqlConn{}

	return
}

func (c *Conn) rollback() (err error) {
	c.Status &= ^mysql.SERVER_STATUS_IN_TRANS

	for _, co := range c.txConns {
		if e := co.Rollback(); e != nil {
			err = e
		}
		co.Close()
	}

	c.txConns = map[*Node]*client.SqlConn{}

	return
}

//if status is in_trans, need
//else if status is not autocommit, need
//else no need
func (c *Conn) needBeginTx() bool {
	return c.isInTransaction() || !c.isAutoCommit()
}
