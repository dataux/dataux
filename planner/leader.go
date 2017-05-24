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
		u.Debugf("leader create %s", string(conf))
		return &LeaderActor{
			client: client,
			peers: &peerList{
				l:       make([]*peerentry, 0),
				entries: make(map[string]*peerentry),
			},
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

	for {
		select {
		case <-c.Done():
			return
		case <-ticker.C:
			// Ask for current peers.
			peers, err := a.client.Query(timeout, grid.Peers)
			if err != nil {
				u.Errorf("Could not query for peers %v", err)
				continue
			}

			for _, e := range a.peers.l {
				e.found = false
			}

			// Check for new peers.
			for _, peer := range peers {
				if e, ok := a.peers.entries[peer.Name()]; ok {
					e.found = true
					continue
				}

				e := &peerentry{
					name:  peer.Name(),
					id:    a.peers.NextId(),
					found: true,
				}
				a.peers.Add(e)

				// Define a sqlworker.
				start := grid.NewActorStart("sqlworker-%d", e.id)
				start.Type = "sqlworker"

				// On new peers start the sqlworker.
				_, err := a.client.Request(timeout, e.name, start)
				if err != nil {
					u.Errorf("could not contact peer %+v  err=%v", e, err)
				}
			}

			// Check for dropped peers
			for _, e := range a.peers.l {
				if !e.found {
					u.Warnf("dropped worker %+v", e)
					a.peers.Remove(e)
				}
			}
		}
	}
}

type peerList struct {
	l       []*peerentry
	entries map[string]*peerentry
}
type peerentry struct {
	name  string
	found bool
	id    int
}

func (s peerList) Remove(e *peerentry) {
	l := make([]*peerentry, 0, len(s.l)-1)
	for _, le := range s.l {
		if le.name != e.name {
			l = append(l, le)
		}
	}
	s.l = l
	delete(s.entries, e.name)
}
func (s peerList) Add(e *peerentry) {
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
