package planner

import (
	"context"
	"sort"
	"sync"
	"time"

	u "github.com/araddon/gou"
	"github.com/lytics/grid"
)

const timeout = 5 * time.Second

var (
	_         = u.EMPTY
	leaderCtx = u.NewContext(context.Background(), "leader-actor")
)

// LeaderCreate factory function to create the Leader
func LeaderCreate(client *grid.Client) grid.MakeActor {
	return func(conf []byte) (grid.Actor, error) {
		return &LeaderActor{
			client: client,
			peers:  newPeerList(leaderCtx),
			logctx: "leader-actor",
		}, nil
	}
}

// LeaderActor is the scheduler to create and watch
// the workers.
type LeaderActor struct {
	client *grid.Client
	peers  *peerList
	logctx string
}

// Act checks for peers, ie: other processes running this code,
// in the same namespace and start the sqlworker actor on each of them.
func (a *LeaderActor) Act(ctx context.Context) {

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	//a.logctx = u.FromContext(ctx)

	u.Debugf("%s leader ACT", a.logctx)

	newPeer := func(e *peerEntry) {

		u.Debugf("%s creating new actor %+v", a.logctx, e)

		// For each Peer/Node create a sqlworker.
		start := grid.NewActorStart("sqlworker-%d", e.id)
		start.Type = "sqlworker"

		// On new peers start the sqlworker.
		_, err := a.client.Request(timeout, e.name, start)
		if err != nil {
			u.Errorf("could not contact peer %+v  err=%v", e, err)
		}
	}

	// long running watch
	//ctx := u.FromContext(ctx, "leader")
	go a.peers.watchPeers(ctx, a.client, newPeer)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// hm
		}
	}
}

type NewPeer func(p *peerEntry)

type peerList struct {
	mu      sync.Mutex
	l       []*peerEntry
	entries map[string]*peerEntry
	idx     int
	logctx  string
}
type peerEntry struct {
	name  string
	found bool
	id    int
}

func newPeerList(ctx context.Context) *peerList {
	return &peerList{
		logctx:  u.FromContext(ctx),
		l:       make([]*peerEntry, 0),
		entries: make(map[string]*peerEntry),
	}
}

func (s *peerList) waitForLoad() {
	s.mu.Lock()
	lc := len(s.l)
	s.mu.Unlock()
	if lc != 0 {
		return
	}

	for i := 0; i < 10; i++ {
		u.Debugf("%s waiting for load", s.logctx)
		time.Sleep(time.Millisecond * 500)
		s.mu.Lock()
		lc = len(s.l)
		s.mu.Unlock()
		if lc > 0 {
			break
		}
	}
	// we are un-locked
	if lc == 0 {
		u.Warnf("No peers")
	}
}

func (s *peerList) GetPeers(ct int) []int {

	s.waitForLoad()
	s.mu.Lock()
	defer s.mu.Unlock()
	// use last index to iterate
	l := make([]int, 0, ct)
	lc := len(s.l)

	if lc == 0 {
		u.Warnf("NO peers?")
		return l
	}
	for i := 0; i < ct; i++ {
		u.Debugf("%s  i=%d  len(l)=%d  ct=%v  idx=%v", s.logctx, i, lc, ct, s.idx)
		p := s.l[s.idx]
		u.Debugf("%+v", p)
		l = append(l, p.id)
		s.idx++
		if s.idx >= lc {
			s.idx = 0
		}
	}
	return l
}

func (p *peerList) watchPeers(ctx context.Context, client *grid.Client, onNew NewPeer) {

	u.Infof("%s %p Starting Peer Watch %#v", p.logctx, p, p)

	// Ask for current peers.
	peers, watch, err := client.QueryWatch(ctx, grid.Peers)
	if err != nil {
		u.Errorf("Not handled query watch error %v", err)
		return
	}

	p.mu.Lock()
	for _, e := range p.l {
		e.found = false
	}

	// Check for new peers.
	for _, peer := range peers {
		if e, ok := p.entries[peer.Name()]; ok {
			e.found = true
			u.Infof("%s found existing peer %s", p.logctx, peer.Name())
			continue
		}

		e := &peerEntry{
			name:  peer.Name(),
			id:    p.nextId(),
			found: true,
		}
		p.add(e)

		u.Debugf("%s %p found new worker sqlworker-%v  %v len(peers)=%d", p.logctx, p, e.id, peer.Name(), len(p.l))

		onNew(e)
	}

	// Check for dropped peers
	for _, e := range p.l {
		if !e.found {
			u.Warnf("dropped worker %+v", e)
			p.remove(e)
		}
	}

	p.mu.Unlock()

	u.Infof("%s %p starting watch events %#v", p.logctx, p, p)

	for {
		select {
		case <-ctx.Done():
			return
		case event := <-watch:
			u.Infof("event %v", event)
			p.mu.Lock()
			u.Infof("event %v", event)
			switch event.Type {
			case grid.WatchError:
				// Error occured watching peers, deal with error.
				u.Errorf("watch error %#v", event)
			case grid.EntityLost:
				// Existing peer lost, reschedule work on extant peers.
				for _, e := range p.l {
					if e.name == event.Peer() {
						p.remove(e)
					}
				}
			case grid.EntityFound:
				peer := event.Peer()
				// New peer found, assign work, get data, reschedule, etc.
				if e, ok := p.entries[peer]; ok {
					e.found = true
				} else {
					e := &peerEntry{
						name:  peer,
						id:    p.nextId(),
						found: true,
					}
					p.add(e)
					u.Debugf("submitting new worker sqlworker-%v  %v", e.id, peer)
					onNew(e)
				}
			}

			p.mu.Unlock()
		}
	}

}

func (s *peerList) remove(e *peerEntry) {
	l := make([]*peerEntry, 0, len(s.l)-1)
	for _, le := range s.l {
		if le.name != e.name {
			l = append(l, le)
		}
	}
	s.l = l
	delete(s.entries, e.name)
}
func (s *peerList) add(e *peerEntry) {
	s.l = append(s.l, e)
	s.entries[e.name] = e
}
func (s *peerList) nextId() int {
	// Make sure they are sorted
	sort.Sort(s)
	if len(s.l) > 0 {
		return s.l[len(s.l)-1].id + 1
	}
	return 1
}
func (s *peerList) Len() int {
	return len(s.l)
}
func (s *peerList) Swap(i, j int) {
	s.l[i], s.l[j] = s.l[j], s.l[i]
}
func (s *peerList) Less(i, j int) bool {
	return s.l[i].id < s.l[j].id
}
