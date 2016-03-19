package proxy

import (
	"fmt"
	"net"
	"runtime"
	"strings"
	"sync/atomic"

	u "github.com/araddon/gou"

	"github.com/dataux/dataux/models"
)

var (
	// Ensure that we implement the interfaces we expect
	_ models.Listener = (*mysqlListener)(nil)

	// or the listener type/name for this frontend connection
	ListenerType = "mysql"

	_ = u.EMPTY
)

func ListenerInit(feConf *models.ListenerConfig, conf *models.Config, sc models.StatementHandlerCreator) (models.Listener, error) {
	return newMysqlListener(feConf, conf, sc)
}

func newMysqlListener(feConf *models.ListenerConfig, conf *models.Config, sc models.StatementHandlerCreator) (*mysqlListener, error) {

	myl := new(mysqlListener)

	myl.sc = sc
	myl.cfg = conf
	myl.feconf = feConf
	myl.addr = feConf.Addr
	myl.user = feConf.User
	myl.password = feConf.Password

	var err error
	netProto := "tcp"
	if strings.Contains(netProto, "/") {
		netProto = "unix"
	}
	myl.netlistener, err = net.Listen(netProto, myl.addr)

	if err != nil {
		return nil, err
	}

	//u.Debugf("Server run MySql Protocol Listen(%s) at '%s'", netProto, myl.addr)
	return myl, nil
}

// mysqlListener implements proxy.Listener interface for
//  running listener connections for mysql
type mysqlListener struct {
	cfg         *models.Config
	feconf      *models.ListenerConfig
	sc          models.StatementHandlerCreator
	handler     interface{}
	connCt      int32 // current connection count
	addr        string
	user        string
	password    string
	running     bool
	netlistener net.Listener
}

func (m *mysqlListener) Init(conf *models.ListenerConfig, svr *models.ServerCtx) error {
	// m.svr = svr
	// m.conf = conf
	u.Warnf("has listener")
	return nil
}

func (m *mysqlListener) Run(stop chan bool) error {

	m.running = true

	for m.running {
		conn, err := m.netlistener.Accept()
		if err != nil {
			u.Errorf("accept error %s", err.Error())
			continue
		}

		go m.OnConn(conn)
	}

	return nil
}

func (m *mysqlListener) Close() error {
	m.running = false
	if m.netlistener != nil {
		return m.netlistener.Close()
	}
	return nil
}

// For each new client tcp connection to this proxy
func (m *mysqlListener) OnConn(c net.Conn) {

	conn := newConn(m, c)

	defer func() {
		if !m.cfg.SupressRecover {
			if err := recover(); err != nil {
				const size = 4096
				buf := make([]byte, size)
				buf = buf[:runtime.Stack(buf, false)]
				u.Errorf("onConn panic %v: %v\n%s", c.RemoteAddr().String(), err, buf)
			}
		}
		atomic.AddInt32(&m.connCt, -1)
		conn.Close()
	}()

	//u.Infof("client connected")
	if err := conn.Handshake(); err != nil {
		u.Errorf("handshake error %s", err.Error())
		c.Close()
		return
	}
	atomic.AddInt32(&m.connCt, 1)
	//u.Debugf("new conn id=%d conns active:%d  p:%p", conn.connectionId, m.connCt, conn)
	// Blocking
	conn.Run()
}

func (m *mysqlListener) UpMaster(node string, addr string) error {

	if shardHandler, ok := m.handler.(*HandlerSharded); ok {
		n := shardHandler.getNode(node)
		if n == nil {
			return fmt.Errorf("invalid node %s", node)
		}
		return n.upMaster(addr)
	} else {
		u.Warnf("UpMaster not implemented for T:%T", m.handler)
	}
	return nil
}

func (m *mysqlListener) UpSlave(node string, addr string) error {

	if shardHandler, ok := m.handler.(*HandlerSharded); ok {
		n := shardHandler.getNode(node)
		if n == nil {
			return fmt.Errorf("invalid node %s", node)
		}

		return n.upSlave(addr)
	} else {
		u.Warnf("UpSlave not implemented for T:%T", m.handler)
	}
	return nil
}
func (m *mysqlListener) DownMaster(node string) error {

	if shardHandler, ok := m.handler.(*HandlerSharded); ok {
		n := shardHandler.getNode(node)
		if n == nil {
			return fmt.Errorf("invalid node %s", node)
		}
		n.db = nil
		return n.downMaster()
	} else {
		u.Warnf("DownMaster not implemented for T:%T", m.handler)
	}
	return nil
}

func (m *mysqlListener) DownSlave(node string) error {

	if shardHandler, ok := m.handler.(*HandlerSharded); ok {
		return nil
		n := shardHandler.getNode(node)
		if n == nil {
			return fmt.Errorf("invalid node [%s].", node)
		}
		return n.downSlave()
	} else {
		u.Warnf("DownSlave not implemented for T:%T", m.handler)
	}
	return nil
}
