package grid

import (
	"context"
	"fmt"
	"time"

	"github.com/lytics/grid/registry"
)

type entityType string

const (
	// Peers filter for query.
	Peers entityType = "peer"
	// Actors filter for query.
	Actors entityType = "actor"
	// Mailboxes filter for query.
	Mailboxes entityType = "mailbox"
)

// EventType categorizing the event.
type EventType int

const (
	WatchError  EventType = 0
	EntityLost  EventType = 1
	EntityFound EventType = 2
)

// QueryEvent indicating that an entity has been discovered,
// lost, or some error has occured with the watch.
type QueryEvent struct {
	name   string
	peer   string
	err    error
	entity entityType
	Type   EventType
}

// Name of entity that caused the event. For example, if
// mailboxes were queried the name is the mailbox name.
func (e *QueryEvent) Name() string {
	return e.name
}

// Peer of named entity. For example, if mailboxes were
// queried then it's the peer the mailbox is running on.
// If the query was for peers, then methods Name and
// Peer return the same string.
func (e *QueryEvent) Peer() string {
	return e.peer
}

// Err caught watching query events. The error is
// not associated with any particular entity, it's
// an error with the watch itself or a result of
// the watch.
func (e *QueryEvent) Err() error {
	return e.err
}

// String representation of query event.
func (e *QueryEvent) String() string {
	if e == nil {
		return "query event: <nil>"
	}
	switch e.Type {
	case EntityLost:
		return fmt.Sprintf("query event: %v lost: %v", e.entity, e.name)
	case EntityFound:
		return fmt.Sprintf("query event: %v found: %v, on peer: %v", e.entity, e.name, e.peer)
	default:
		return fmt.Sprintf("query event: error: %v", e.err)
	}
}

// QueryWatch monitors the entry and exit of peers, actors, or mailboxes.
//
// Example usage:
//
//     client, err := grid.NewClient(...)
//     ...
//
//     currentpeers, watch, err := client.QueryWatch(ctx, grid.Peers)
//     ...
//
//     for _, peer := range currentpeers {
//         // Do work regarding peer.
//     }
//
//     for event := range watch {
//         switch event.Type {
//         case grid.WatchError:
//             // Error occured watching peers, deal with error.
//         case grid.EntityLost:
//             // Existing peer lost, reschedule work on extant peers.
//         case grid.EntityFound:
//             // New peer found, assign work, get data, reschedule, etc.
//         }
//     }
func (c *Client) QueryWatch(ctx context.Context, filter entityType) ([]*QueryEvent, <-chan *QueryEvent, error) {
	nsName, err := namespacePrefix(filter, c.cfg.Namespace)
	if err != nil {
		return nil, nil, err
	}

	regs, changes, err := c.registry.Watch(ctx, nsName)
	var current []*QueryEvent
	for _, reg := range regs {
		current = append(current, &QueryEvent{
			name:   nameFromKey(filter, c.cfg.Namespace, reg.Key),
			peer:   reg.Registry,
			entity: filter,
			Type:   EntityFound,
		})
	}

	queryEvents := make(chan *QueryEvent)
	put := func(change *QueryEvent) {
		select {
		case <-ctx.Done():
		case queryEvents <- change:
		}
	}
	putTerminalError := func(change *QueryEvent) {
		go func() {
			select {
			case <-time.After(10 * time.Minute):
			case queryEvents <- change:
			}
		}()
	}
	go func() {
		for {
			select {
			case change, open := <-changes:
				if !open {
					select {
					case <-ctx.Done():
					default:
						putTerminalError(&QueryEvent{err: ErrWatchClosedUnexpectedly})
					}
					return
				}
				if change.Error != nil {
					putTerminalError(&QueryEvent{err: change.Error})
					return
				}
				switch change.Type {
				case registry.Delete:
					qe := &QueryEvent{
						name:   nameFromKey(filter, c.cfg.Namespace, change.Key),
						entity: filter,
						Type:   EntityLost,
					}
					// Maintain contract that for peer events
					// the Peer() and Name() methods return
					// the same value.
					//
					// Also keep in mind that when the grid
					// library registers a "peer", the peer
					// name is in fact the string returned by
					// the registry.Registry() method.
					if filter == Peers {
						qe.peer = qe.name
					}
					put(qe)
				case registry.Create, registry.Modify:
					qe := &QueryEvent{
						name:   nameFromKey(filter, c.cfg.Namespace, change.Key),
						peer:   change.Reg.Registry,
						entity: filter,
						Type:   EntityFound,
					}
					// Maintain contract that for peer events
					// the Peer() and Name() methods return
					// the same value.
					//
					// Also keep in mind that when the grid
					// library registers a "peer", the peer
					// name is in fact the string returned by
					// the registry.Registry() method.
					if filter == Peers {
						qe.peer = qe.name
					}
					put(qe)
				}
			}
		}
	}()

	return current, queryEvents, nil
}

// Query in this client's namespace. The filter can be any one of
// Peers, Actors, or Mailboxes.
func (c *Client) Query(timeout time.Duration, filter entityType) ([]*QueryEvent, error) {
	timeoutC, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return c.QueryC(timeoutC, filter)
}

// QueryC (query) in this client's namespace. The filter can be any
// one of Peers, Actors, or Mailboxes. The context can be used to
// control cancelation or timeouts.
func (c *Client) QueryC(ctx context.Context, filter entityType) ([]*QueryEvent, error) {
	nsPrefix, err := namespacePrefix(filter, c.cfg.Namespace)
	if err != nil {
		return nil, err
	}
	regs, err := c.registry.FindRegistrations(ctx, nsPrefix)
	if err != nil {
		return nil, err
	}

	var result []*QueryEvent
	for _, reg := range regs {
		result = append(result, &QueryEvent{
			name:   nameFromKey(filter, c.cfg.Namespace, reg.Key),
			peer:   reg.Registry,
			entity: filter,
			Type:   EntityFound,
		})
	}

	return result, nil
}

// nameFromKey returns the name from the data field of a registration.
// Used by query to return just simple string data.
func nameFromKey(filter entityType, namespace string, key string) string {
	name, err := stripNamespace(filter, namespace, key)
	// INVARIANT
	// Under all circumstances if a registration is returned
	// from the prefix scan above, ie: FindRegistrations,
	// then each registration must contain the namespace
	// as a prefix of the key.
	if err != nil {
		panic("registry key without proper namespace prefix: " + key)
	}
	return name
}
