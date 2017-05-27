package planner

import (
	"context"
	"sort"
	"time"

	u "github.com/araddon/gou"
	"github.com/lytics/grid/grid.v3"
)

const timeout = 5 * time.Second

var (
	_ = u.EMPTY
)

// LeaderCreate factory function to create the Leader
func LeaderCreate(client *grid.Client) grid.MakeActor {
	return func(conf []byte) (grid.Actor, error) {
		return &LeaderActor{
			client: client,
			peers:  newPeerList(),
		}, nil
	}
}

// LeaderActor is the scheduler to create and watch
// the workers.
type LeaderActor struct {
	client *grid.Client
	peers  *peerList
}

// Act checks for peers, ie: other processes running this code,
// in the same namespace and start the sqlworker actor on each of them.
func (a *LeaderActor) Act(c context.Context) {

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	//u.Debugf("leader ACT")

	newPeer := func(e *peerEntry) {
		// Define a sqlworker.
		start := grid.NewActorStart("sqlworker-%d", e.id)
		start.Type = "sqlworker"

		// On new peers start the sqlworker.
		_, err := a.client.Request(timeout, e.name, start)
		if err != nil {
			u.Errorf("could not contact peer %+v  err=%v", e, err)
		}
	}

	// long running watch
	go a.peers.watchPeers(c, a.client, newPeer)

	for {
		select {
		case <-c.Done():
			return
		case <-ticker.C:
			// hm
		}
	}
}

type NewPeer func(p *peerEntry)

type peerList struct {
	l       []*peerEntry
	entries map[string]*peerEntry
}
type peerEntry struct {
	name  string
	found bool
	id    int
}

func newPeerList() *peerList {
	return &peerList{
		l:       make([]*peerEntry, 0),
		entries: make(map[string]*peerEntry),
	}
}

func (p *peerList) watchPeers(ctx context.Context, client *grid.Client, onNew NewPeer) {

	//u.Infof("Starting Peer Watch %#v", p)

	// Ask for current peers.
	peers, watch, err := client.QueryWatch(ctx, grid.Peers)
	if err != nil {
		u.Errorf("Not handled query watch error %v", err)
		return
	}

	for _, e := range p.l {
		e.found = false
	}

	// Check for new peers.
	for _, peer := range peers {
		if e, ok := p.entries[peer.Name()]; ok {
			e.found = true
			continue
		}

		e := &peerEntry{
			name:  peer.Name(),
			id:    p.NextId(),
			found: true,
		}
		p.Add(e)

		u.Debugf("found new worker sqlworker-%v  %v", e.id, peer.Name())

		onNew(e)
	}

	// Check for dropped peers
	for _, e := range p.l {
		if !e.found {
			u.Warnf("dropped worker %+v", e)
			p.Remove(e)
		}
	}

	for event := range watch {
		switch event.Type {
		case grid.WatchError:
			// Error occured watching peers, deal with error.
			u.Errorf("watch error %#v", event)
		case grid.EntityLost:
			// Existing peer lost, reschedule work on extant peers.
			for _, e := range p.l {
				if e.name == event.Peer() {
					p.Remove(e)
				}
			}
		case grid.EntityFound:
			peer := event.Peer()
			// New peer found, assign work, get data, reschedule, etc.
			if e, ok := p.entries[peer]; ok {
				e.found = true
				continue
			}

			e := &peerEntry{
				name:  peer,
				id:    p.NextId(),
				found: true,
			}
			p.Add(e)
			u.Debugf("submitting new worker sqlworker-%v  %v", e.id, peer)
			onNew(e)
		}
	}

}

func (s peerList) Remove(e *peerEntry) {
	l := make([]*peerEntry, 0, len(s.l)-1)
	for _, le := range s.l {
		if le.name != e.name {
			l = append(l, le)
		}
	}
	s.l = l
	delete(s.entries, e.name)
}
func (s peerList) Add(e *peerEntry) {
	s.l = append(s.l, e)
	s.entries[e.name] = e
}
func (s peerList) NextId() int {
	// Make sure they are sorted
	sort.Sort(s)
	if len(s.l) > 0 {
		return s.l[len(s.l)-1].id + 1
	}
	return 1
}
func (s peerList) Len() int {
	return len(s.l)
}
func (s peerList) Swap(i, j int) {
	s.l[i], s.l[j] = s.l[j], s.l[i]
}
func (s peerList) Less(i, j int) bool {
	return s.l[i].id < s.l[j].id
}
