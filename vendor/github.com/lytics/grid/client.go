package grid

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	etcdv3 "github.com/coreos/etcd/clientv3"
	"github.com/lytics/grid/codec"
	"github.com/lytics/grid/registry"
	"github.com/lytics/retry"
	"google.golang.org/grpc"
)

// Register a message so it may be sent and received.
// Value v should not be a pointer to a type, but
// the type itself.
//
// For example:
//     Register(MyMsg{})    // Correct
//     Register(&MyMsg{})   // Incorrect
//
func Register(v interface{}) error {
	return codec.Register(v)
}

//clientAndConnPool is a pool of clientAndConn
type clientAndConnPool struct {
	// The 'id' is used in a kind of CAS when
	// deleting the client pool. This allows
	// users of the pool to delete it only
	// if the same client is being referenced.
	// See the function deleteClientAndConn
	// for more details.
	id          int64
	incr        int
	clientConns []*clientAndConn
}

func (ccp *clientAndConnPool) next() (*clientAndConn, error) {
	// Testing hook, used easily check
	// a code path in the client.
	if ccp == nil || len(ccp.clientConns) == 0 {
		return nil, fmt.Errorf("client and conn pool is nil")
	}
	if len(ccp.clientConns) == 0 {
		return nil, fmt.Errorf("client and conn pool is empty")
	}

	idx := ccp.incr % len(ccp.clientConns)
	ccp.incr++
	return ccp.clientConns[idx], nil
}

func (ccp *clientAndConnPool) close() error {
	// Testing hook, used easily check
	// a code path in the client.
	if ccp == nil {
		return fmt.Errorf("client and conn pool is nil")
	}

	var err error
	for _, cc := range ccp.clientConns {
		closeErr := cc.close()
		if closeErr != nil {
			err = closeErr
		}
	}
	return err
}

// clientAndConn of the generated gRPC client
// plus the actual gRPC client connection.
type clientAndConn struct {
	conn   *grpc.ClientConn
	client WireClient
}

// close the gRPC connection.
func (cc *clientAndConn) close() error {
	// Testing hook, used easily check
	// a code path in the client.
	if cc == nil {
		return fmt.Errorf("client and conn is nil")
	}
	return cc.conn.Close()
}

// Client for grid-actors or non-actors to make requests to grid-actors.
// The client can be used by multiple go-routines.
type Client struct {
	mu              sync.Mutex
	cfg             ClientCfg
	registry        *registry.Registry
	addresses       map[string]string
	clientsAndConns map[string]*clientAndConnPool
	// Test hook.
	cs *clientStats
}

// NewClient using the given etcd client and configuration.
func NewClient(etcd *etcdv3.Client, cfg ClientCfg) (*Client, error) {
	setClientCfgDefaults(&cfg)

	r, err := registry.New(etcd)
	if err != nil {
		return nil, err
	}
	r.Timeout = cfg.Timeout

	// Set registry logger.
	if cfg.Logger != nil {
		r.Logger = cfg.Logger
	}

	return &Client{
		cfg:             cfg,
		registry:        r,
		addresses:       make(map[string]string),
		clientsAndConns: make(map[string]*clientAndConnPool),
	}, nil
}

// Close all outbound connections of this client immediately.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var err error
	for _, ccpool := range c.clientsAndConns {
		closeErr := ccpool.close()
		if closeErr != nil {
			err = closeErr
		}
	}
	return err
}

// Request a response for the given message.
func (c *Client) Request(timeout time.Duration, receiver string, msg interface{}) (interface{}, error) {
	timeoutC, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return c.RequestC(timeoutC, receiver, msg)
}

// RequestC (request) a response for the given message. The context can be
// used to control cancelation or timeouts.
func (c *Client) RequestC(ctx context.Context, receiver string, msg interface{}) (interface{}, error) {
	// Namespaced receiver name.
	nsReceiver, err := namespaceName(Mailboxes, c.cfg.Namespace, receiver)
	if err != nil {
		return nil, err
	}

	typeName, data, err := codec.Marshal(msg)
	if err != nil {
		return nil, err
	}

	req := &Delivery{
		Ver:      Delivery_V1,
		Data:     data,
		TypeName: typeName,
		Receiver: nsReceiver,
	}

	var res *Delivery
	retry.X(3, 1*time.Second, func() bool {
		var client WireClient
		var clientID int64
		client, clientID, err = c.getWireClient(ctx, nsReceiver)
		if err != nil && strings.Contains(err.Error(), ErrUnregisteredMailbox.Error()) {
			// Test hook.
			c.cs.Inc(numErrUnregisteredMailbox)
			// Receiver is currently unregistered, so
			// clear them out of the cache and don't
			// try finding them again.
			c.deleteAddress(nsReceiver)
			return false
		}
		if err != nil {
			return false
		}
		res, err = client.Process(ctx, req)
		if err != nil && strings.Contains(err.Error(), "the client connection is closing") {
			// Test hook.
			c.cs.Inc(numErrClientConnectionClosing)
			// The request is via a client that is
			// closing and gRPC is reporting that
			// a request is not a valid operation.
			c.deleteClientAndConn(nsReceiver, clientID)
			select {
			case <-ctx.Done():
				return false
			default:
				return true
			}
		}
		if err != nil && strings.Contains(err.Error(), "the connection is unavailable") {
			// Test hook.
			c.cs.Inc(numErrConnectionUnavailable)
			// Receiver is on a host that may have died.
			// The error "connection is unavailable"
			// comes from gRPC itself. In such a case
			// it's best to try and replace the client.
			c.deleteClientAndConn(nsReceiver, clientID)
			select {
			case <-ctx.Done():
				return false
			default:
				return true
			}
		}
		if err != nil && strings.Contains(err.Error(), ErrUnknownMailbox.Error()) {
			// Test hook.
			c.cs.Inc(numErrUnknownMailbox)
			// Receiver possibly moved to different
			// host for one reason or another. Get
			// rid of old address and try discovering
			// new host, and send again.
			c.deleteAddress(nsReceiver)
			select {
			case <-ctx.Done():
				return false
			default:
				return true
			}
		}
		if err != nil && strings.Contains(err.Error(), ErrReceiverBusy.Error()) {
			// Test hook.
			c.cs.Inc(numErrReceiverBusy)
			// Receiver was busy, ie: the receiving channel
			// was at capacity. Also, the reciever definitely
			// did NOT get the message, so there is no risk
			// of duplication if the request is tried again.
			select {
			case <-ctx.Done():
				return false
			default:
				return true
			}
		}
		return false
	})
	if err != nil {
		return nil, err
	}

	reply, err := codec.Unmarshal(res.Data, res.TypeName)
	if err != nil {
		return nil, err
	}

	return reply, nil
}

// getWireClient for the address of the receiver.
func (c *Client) getWireClient(ctx context.Context, nsReceiver string) (WireClient, int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	const noID = -1

	// Test hook.
	c.cs.Inc(numGetWireClient)

	address, ok := c.addresses[nsReceiver]
	if !ok {
		reg, err := c.registry.FindRegistration(ctx, nsReceiver)
		if err != nil && err == registry.ErrUnknownKey {
			return nil, noID, ErrUnregisteredMailbox
		}
		if err != nil {
			return nil, noID, err
		}
		address = reg.Address
		c.addresses[nsReceiver] = address
	}

	ccpool, ok := c.clientsAndConns[address]
	if !ok {
		ccpool = &clientAndConnPool{id: rand.Int63(), clientConns: make([]*clientAndConn, c.cfg.ConnectionsPerPeer)}
		for i := 0; i < c.cfg.ConnectionsPerPeer; i++ {
			// Test hook.
			c.cs.Inc(numGRPCDial)

			// Dial the destination.
			conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBackoffMaxDelay(20*time.Second))
			if err != nil {
				return nil, noID, err
			}
			client := NewWireClient(conn)
			cc := &clientAndConn{
				conn:   conn,
				client: client,
			}
			ccpool.clientConns[i] = cc
		}
		c.clientsAndConns[address] = ccpool
	}
	cc, err := ccpool.next()
	if err != nil {
		return nil, noID, err
	}
	return cc.client, ccpool.id, nil
}

func (c *Client) deleteAddress(nsReceiver string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Test hook.
	c.cs.Inc(numDeleteAddress)

	delete(c.addresses, nsReceiver)
}

func (c *Client) deleteClientAndConn(nsReceiver string, clientID int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Test hook.
	c.cs.Inc(numDeleteClientAndConn)

	address, ok := c.addresses[nsReceiver]
	if !ok {
		return
	}
	delete(c.addresses, nsReceiver)

	ccpool, ok := c.clientsAndConns[address]
	if !ok {
		return
	}
	// Between the time this client was gotten
	// and this delete operation, someone has
	// already changed it out from under this
	// caller, so just ignore the delete.
	if clientID != ccpool.id {
		return
	}
	err := ccpool.close()
	if err != nil {
		c.logf("error closing client and connection: %v", err)
	}
	delete(c.clientsAndConns, address)
}

func (c *Client) logf(format string, v ...interface{}) {
	if c.cfg.Logger != nil {
		c.cfg.Logger.Printf(format, v...)
	}
}

// statName of interesting statistic to track
// during testing for validation.
type statName string

const (
	numErrClientConnectionClosing statName = "numErrClientConnectionClosing"
	numErrConnectionUnavailable   statName = "numErrConnectionUnavailable"
	numErrUnregisteredMailbox     statName = "numErrUnregisteredMailbox"
	numErrUnknownMailbox          statName = "numErrUnknownMailbox"
	numErrReceiverBusy            statName = "numErrReceiverBusy"
	numDeleteAddress              statName = "numDeleteAddress"
	numDeleteClientAndConn        statName = "numDeleteClientAndConn"
	numGetWireClient              statName = "numGetWireClient"
	numGRPCDial                   statName = "numGRPCDial"
)

// newClientStats for use during testing.
func newClientStats() *clientStats {
	return &clientStats{
		counters: map[statName]int{},
	}
}

// clientStats is a test hook.
type clientStats struct {
	mu       sync.Mutex
	counters map[statName]int
}

// Add to the counter.
func (cs *clientStats) Inc(name statName) {
	if cs == nil {
		return
	}
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.counters[name]++
}

// String of client stats.
func (cs *clientStats) String() string {
	var buf bytes.Buffer
	var i int
	for name, stat := range cs.counters {
		buf.WriteString(string(name))
		buf.WriteString(":")
		buf.WriteString(strconv.Itoa(stat))
		if i+1 < len(cs.counters) {
			buf.WriteString(", ")
			i++
		}
	}
	return buf.String()
}
