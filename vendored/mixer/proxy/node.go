package proxy

import (
	"fmt"
	"sync"
	"time"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/schema"
	"github.com/dataux/dataux/vendored/mixer/client"
)

const (
	Master = "master"
	Slave  = "slave"
)

// Node describes a backend server endpoint and is responsible for
// creating connections to these backends as well as monitoring health
// of them
type Node struct {
	sync.Mutex

	conf *schema.SourceConfig

	// client.DB's are connection managers
	// each client.DB represents a server/address/database combo
	// and its GetConn() will return a pooled connection to db
	db     *client.DB
	master *client.DB
	slave  *client.DB

	downAfterNoAlive time.Duration

	lastMasterPing int64
	lastSlavePing  int64
}

func (n *Node) run() {
	//to do
	//1 check connection alive
	//2 check remove mysql server alive

	t := time.NewTicker(3000 * time.Second)
	defer t.Stop()

	n.lastMasterPing = time.Now().Unix()
	n.lastSlavePing = n.lastMasterPing
	for {
		select {
		case <-t.C:
			n.checkMaster()
			n.checkSlave()
		}
	}
}

func (n *Node) String() string {
	return n.conf.Name
}

func (n *Node) getMasterConn() (*client.SqlConn, error) {
	n.Lock()
	db := n.db
	n.Unlock()

	if db == nil {
		return nil, fmt.Errorf("master is down")
	}
	//u.Debugf("about to GetConn:   client.SqlConn")
	return db.GetConn()
}

func (n *Node) getSelectConn() (*client.SqlConn, error) {
	var db *client.DB

	rwSplit := n.conf.Settings.Bool("rwsplit")
	n.Lock()
	if rwSplit && n.slave != nil {
		db = n.slave
	} else {
		db = n.db
	}
	n.Unlock()

	if db == nil {
		return nil, fmt.Errorf("no alive mysql server")
	}

	return db.GetConn()
}

func (n *Node) checkMaster() {
	n.Lock()
	db := n.db
	n.Unlock()

	if db == nil {
		u.Infof("no master avaliable")
		return
	}

	if err := db.Ping(); err != nil {
		u.Errorf("%s ping master %s error %s", n, db.Addr(), err.Error())
	} else {
		n.lastMasterPing = time.Now().Unix()
		return
	}

	if int64(n.downAfterNoAlive) > 0 && time.Now().Unix()-n.lastMasterPing > int64(n.downAfterNoAlive) {
		u.Errorf("%s down master db %s", n, n.master.Addr())

		n.downMaster()
	}
}

func (n *Node) checkSlave() {
	if n.slave == nil {
		return
	}

	db := n.slave
	if err := db.Ping(); err != nil {
		u.Errorf("%s ping slave %s error %s", n, db.Addr(), err.Error())
	} else {
		n.lastSlavePing = time.Now().Unix()
	}

	if int64(n.downAfterNoAlive) > 0 && time.Now().Unix()-n.lastSlavePing > int64(n.downAfterNoAlive) {
		u.Errorf("%s slave db %s not alive over %ds, down it",
			n, db.Addr(), int64(n.downAfterNoAlive/time.Second))

		n.downSlave()
	}
}

func (n *Node) openDB(addr string) (*client.DB, error) {
	user := n.conf.Settings.String("user")
	password := n.conf.Settings.String("password")
	db, err := client.Open(addr, user, password, "")
	if err != nil {
		return nil, err
	}
	idelConns := n.conf.Settings.Int("idleconns")
	db.SetMaxIdleConnNum(idelConns)
	return db, nil
}

func (n *Node) checkUpDB(addr string) (*client.DB, error) {
	db, err := n.openDB(addr)
	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}

	return db, nil
}

func (n *Node) upMaster(addr string) error {
	n.Lock()
	if n.master != nil {
		n.Unlock()
		return fmt.Errorf("%s master must be down first", n)
	}
	n.Unlock()

	db, err := n.checkUpDB(addr)
	if err != nil {
		return err
	}

	n.Lock()
	n.master = db
	n.db = db
	n.Unlock()

	return nil
}

func (n *Node) upSlave(addr string) error {
	n.Lock()
	if n.slave != nil {
		n.Unlock()
		return fmt.Errorf("%s, slave must be down first", n)
	}
	n.Unlock()

	db, err := n.checkUpDB(addr)
	if err != nil {
		return err
	}

	n.Lock()
	n.slave = db
	n.Unlock()

	return nil
}

func (n *Node) downMaster() error {
	n.Lock()
	if n.master != nil {
		n.master = nil
	}
	return nil
}

func (n *Node) downSlave() error {
	n.Lock()
	db := n.slave
	n.slave = nil
	n.Unlock()

	if db != nil {
		db.Close()
	}

	return nil
}
