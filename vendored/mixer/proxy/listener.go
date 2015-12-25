package proxy

import (
	"fmt"
	"net"
	"runtime"
	"strings"

	u "github.com/araddon/gou"
	"github.com/dataux/dataux/pkg/models"
)

var (
	// Ensure that we implement the interfaces we expect
	_ models.Listener = (*MysqlListener)(nil)

	// The "backend_type" for backends
	// or the listener type for frontends
	ListenerType = "mysql"

	_ = u.EMPTY
)

func ListenerInit(feConf *models.ListenerConfig, conf *models.Config) (models.Listener, error) {
	return NewMysqlListener(feConf, conf)
}

func NewMysqlListener(feConf *models.ListenerConfig, conf *models.Config) (*MysqlListener, error) {

	myl := new(MysqlListener)

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

// MysqlListener implements proxy.Listener interface for
//  running listener connections for mysql
type MysqlListener struct {
	cfg         *models.Config
	feconf      *models.ListenerConfig
	connCt      int
	addr        string
	user        string
	password    string
	running     bool
	netlistener net.Listener
	handle      models.ConnectionHandle
}

func (m *MysqlListener) Run(handle models.ConnectionHandle, stop chan bool) error {

	m.handle = handle
	//u.Debugf("using handler:  %T", handle)
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

func (m *MysqlListener) Close() error {
	m.running = false
	if m.netlistener != nil {
		return m.netlistener.Close()
	}
	return nil
}

// For each new client tcp connection to this proxy
func (m *MysqlListener) OnConn(c net.Conn) {

	conn := newConn(m, c)
	u.Debugf("new conn id=%d p:%p", m.connCt, conn)

	defer func() {
		if !m.cfg.SupressRecover {
			if err := recover(); err != nil {
				const size = 4096
				buf := make([]byte, size)
				buf = buf[:runtime.Stack(buf, false)]
				u.Errorf("onConn panic %v: %v\n%s", c.RemoteAddr().String(), err, buf)
			}
		}
		m.connCt--
		conn.Close()
	}()

	//u.Infof("client connected")
	if err := conn.Handshake(); err != nil {
		u.Errorf("handshake error %s", err.Error())
		c.Close()
		return
	}
	m.connCt++
	// Blocking
	conn.Run()
}

func (m *MysqlListener) UpMaster(node string, addr string) error {

	if shardHandler, ok := m.handle.(*HandlerSharded); ok {
		n := shardHandler.getNode(node)
		if n == nil {
			return fmt.Errorf("invalid node %s", node)
		}
		return n.upMaster(addr)
	} else {
		u.Warnf("UpMaster not implemented for T:%T", m.handle)
	}
	return nil
}

func (m *MysqlListener) UpSlave(node string, addr string) error {

	if shardHandler, ok := m.handle.(*HandlerSharded); ok {
		n := shardHandler.getNode(node)
		if n == nil {
			return fmt.Errorf("invalid node %s", node)
		}

		return n.upSlave(addr)
	} else {
		u.Warnf("UpSlave not implemented for T:%T", m.handle)
	}
	return nil
}
func (m *MysqlListener) DownMaster(node string) error {

	if shardHandler, ok := m.handle.(*HandlerSharded); ok {
		n := shardHandler.getNode(node)
		if n == nil {
			return fmt.Errorf("invalid node %s", node)
		}
		n.db = nil
		return n.downMaster()
	} else {
		u.Warnf("DownMaster not implemented for T:%T", m.handle)
	}
	return nil
}

func (m *MysqlListener) DownSlave(node string) error {

	if shardHandler, ok := m.handle.(*HandlerSharded); ok {
		return nil
		n := shardHandler.getNode(node)
		if n == nil {
			return fmt.Errorf("invalid node [%s].", node)
		}
		return n.downSlave()
	} else {
		u.Warnf("DownSlave not implemented for T:%T", m.handle)
	}
	return nil
}
