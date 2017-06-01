package planner

import (
	"context"
	"fmt"
	"net"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	u "github.com/araddon/gou"
	etcdv3 "github.com/coreos/etcd/clientv3"
	"github.com/lytics/grid"
	"github.com/sony/sonyflake"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/plan"
)

var (
	// BuiltIn Default Conf, used for testing but real runtime swaps this out
	// for a real config
	GridConf = &Conf{
		GridName:    "dataux",
		Address:     "localhost:0",
		EtcdServers: strings.Split("http://127.0.0.1:2379", ","),
	}

	// Unique id service
	sf *sonyflake.Sonyflake

	// Default mailbox count
	mailboxCount = 10
)

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	var st sonyflake.Settings
	// TODO, ensure we get a unique etcdid for machineid
	st.StartTime = time.Now()
	sf = sonyflake.NewSonyflake(st)
	// Lets use our distributed generator
	plan.NextId = NextIdUnsafe
}

func setupLogging() {
	u.DiscardStandardLogger() // Discard non-sanctioned spammers
}

func NextIdUnsafe() uint64 {
	uv, err := NextId()
	if err != nil {
		u.Errorf("error generating nextId %v", err)
	}
	return uv
}

func NextId() (uint64, error) {
	return sf.NextID()
}

func NodeName(id uint64) string {
	hostname, err := os.Hostname()
	if err != nil {
		u.Errorf("error: failed to discover hostname: %v", err)
	}
	return fmt.Sprintf("%s-%d", hostname, id)
}

func NodeName2(id1, id2 uint64) string {
	hostname, err := os.Hostname()
	if err != nil {
		u.Errorf("error: failed to discover hostname: %v", err)
	}
	return fmt.Sprintf("%s-%d-%d", hostname, id1, id2)
}

// PlannerGrid Is a singleton service context per process that
// manages access to registry, and other singleton resources.
// It starts the workers, grid processes, watch to ensure it
// knows about the rest of the peers in the system.
type PlannerGrid struct {
	Conf       *Conf
	reg        *datasource.Registry
	GridServer *grid.Server
	gridClient *grid.Client
	started    bool
	lastTaskId uint64
	mu         sync.Mutex
	mailboxes  *mailboxPool
	peers      *peerList
}

func NewPlannerGrid(nodeCt int, r *datasource.Registry) *PlannerGrid {

	nextId, _ := NextId()
	conf := GridConf.Clone()
	conf.NodeCt = nodeCt
	conf.Hostname = NodeName(nextId)
	ctx := u.NewContext(context.Background(), "planner-grid")

	return &PlannerGrid{
		Conf:  conf,
		reg:   r,
		peers: newPeerList(ctx),
	}
}

func (m *PlannerGrid) Run(quit chan bool) error {

	logger := u.GetLogger()

	// Connect to etcd.
	etcd, err := etcdv3.New(etcdv3.Config{Endpoints: m.Conf.EtcdServers})
	if err != nil {
		u.Errorf("failed to start etcd client: %v", err)
		return err
	}

	// Create a grid client.
	m.gridClient, err = grid.NewClient(etcd, grid.ClientCfg{Namespace: "dataux", Logger: logger})
	if err != nil {
		u.Errorf("failed to start grid client: %v", err)
		return err
	}

	// Create a grid server.
	m.GridServer, err = grid.NewServer(etcd, grid.ServerCfg{Namespace: "dataux", Logger: logger})
	if err != nil {
		u.Errorf("failed to start grid server: %v", err)
		return err
	}

	// Define how actors are created.
	m.GridServer.RegisterDef("leader", LeaderCreate(m.gridClient))
	m.GridServer.RegisterDef("sqlworker", WorkerFactory(m.Conf, m.gridClient, m.GridServer))

	lis, err := net.Listen("tcp", m.Conf.Address)
	if err != nil {
		u.Errorf("failed to start tcp listener server: %v", err)
		return err
	}

	go func() {
		time.Sleep(time.Millisecond * 100)
		u.Infof("starting mailboxes")
		m.startMailboxes()
		u.Infof("Watch for peers")
		go m.watchPeers()

	}()
	u.Warnf("Starting Grid %q", m.Conf.Hostname)
	// Blocking call to serve
	err = m.GridServer.Serve(lis)
	if err != nil {
		u.Errorf("grid serve failed with error=%v", err)
		return err
	}
	return nil
}

func (m *PlannerGrid) watchPeers() {
	newPeer := func(e *peerEntry) {
		u.Infof("new actor %+v", e)
	}

	ctx, _ := context.WithCancel(context.Background())
	ctx = u.NewContext(ctx, "planner-grid")

	// long running watch
	m.peers.watchPeers(ctx, m.gridClient, newPeer)
}

// GetMailbox get next available mailbox, throttled
func (m *PlannerGrid) GetMailbox() (*grid.Mailbox, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.mailboxes.ready {
		for i := 0; i < 10; i++ {
			time.Sleep(time.Millisecond * 200)
			if m.mailboxes.ready {
				break
			}
		}
		if !m.mailboxes.ready {
			return nil, fmt.Errorf("Mailboxes not ready")
		}
	}
	return m.mailboxes.getNext(), nil
}

// CheckinMailbox return mailbox
func (m *PlannerGrid) CheckinMailbox(mb *grid.Mailbox) {
	id := fmt.Sprintf("%p", mb)
	m.mu.Lock()
	idx := m.mailboxes.ids[id]
	m.mailboxes.next <- idx
	m.mu.Unlock()
}

func (m *PlannerGrid) startMailboxes() {
	m.mu.Lock()
	defer m.mu.Unlock()
	var err error
	size := mailboxCount
	if m.Conf.MailboxCount > 0 {
		size = m.Conf.MailboxCount
	}
	for i := 0; i < 10; i++ {
		// Lets create mailbox pool
		m.mailboxes, err = newPool(m.GridServer, size, fmt.Sprintf("%s-mb", m.Conf.Hostname))
		if err != nil && strings.Contains(err.Error(), "not running") {
			u.Infof("grid server not ready, sleeping %d", i)
			time.Sleep(time.Millisecond * 200)
			continue
		} else if err != nil {
			u.Warnf("wtf? %v", err)
			break
		} else {
			//u.Debugf("nice, created mailboxes")
			m.mailboxes.ready = true
			break
		}
	}
}

type mailboxPool struct {
	mailboxes []*grid.Mailbox
	ids       map[string]int
	next      chan int
	mu        sync.Mutex
	ready     bool
	idx       int
	prefix    string
}

func newPool(s *grid.Server, size int, prefix string) (*mailboxPool, error) {

	p := &mailboxPool{
		mailboxes: make([]*grid.Mailbox, size),
		prefix:    prefix,
		next:      make(chan int, size),
		ids:       make(map[string]int, size),
	}

	for i := 0; i < size; i++ {
		mailbox, err := grid.NewMailbox(s, fmt.Sprintf("%s-%d", prefix, i), 10)
		if err != nil {
			return nil, err
		}
		p.mailboxes[i] = mailbox
		id := fmt.Sprintf("%p", mailbox)
		//u.Debugf("started mailbox  P:%s Id():%v  i:%d", id, mailbox.Id(), i)
		p.ids[id] = i
		p.next <- i
	}

	return p, nil
}

// // CheckinMailbox return mailbox
// func (p *mailboxPool) Checkin(mb *grid.Mailbox) {
// 	id := fmt.Sprintf("%p", mb)
// 	m.mu.Lock()
// 	idx := m.ids[id]
// 	m.next <- idx
// 	m.mu.Unlock()
// }

func (p *mailboxPool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	var err error
	for _, mb := range p.mailboxes {
		if e := mb.Close(); e != nil {
			err = e
		}
	}
	return err
}

func (p *mailboxPool) getNext() *grid.Mailbox {
	i := <-p.next
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.mailboxes[i]
}
