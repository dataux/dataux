package grid

import (
	"context"
	"fmt"
	"net"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	etcdv3 "github.com/coreos/etcd/clientv3"
	"github.com/lytics/grid/codec"
	"github.com/lytics/grid/registry"
	netcontext "golang.org/x/net/context"
	"google.golang.org/grpc"
)

const (
	contextKey = "grid-context-key-xboKEsHA26"
)

type contextVal struct {
	server    *Server
	actorID   string
	actorName string
}

// Server of a grid.
type Server struct {
	mu        sync.Mutex
	ctx       context.Context
	cancel    func()
	cfg       ServerCfg
	etcd      *etcdv3.Client
	grpc      *grpc.Server
	stop      sync.Once
	fatalErr  chan error
	finalErr  error
	actors    map[string]MakeActor
	registry  *registry.Registry
	mailboxes map[string]*Mailbox
}

// NewServer for the grid. The namespace must contain only characters
// in the set: [a-zA-Z0-9-_] and no other.
func NewServer(etcd *etcdv3.Client, cfg ServerCfg) (*Server, error) {
	setServerCfgDefaults(&cfg)

	if !isNameValid(cfg.Namespace) {
		return nil, ErrInvalidNamespace
	}
	if etcd == nil {
		return nil, ErrNilEtcd
	}
	return &Server{
		cfg:      cfg,
		etcd:     etcd,
		grpc:     grpc.NewServer(),
		actors:   map[string]MakeActor{},
		fatalErr: make(chan error, 1),
	}, nil
}

// RegisterDef of an actor. When a ActorStart message is sent to
// a peer it will use the registered definitions to make and run
// the actor. If an actor with actorType "leader" is registered
// it will be started automatically when the Serve method is
// called.
func (s *Server) RegisterDef(actorType string, f MakeActor) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.actors[actorType] = f
}

// Context of the server, when it reports done the
// server is trying to shutdown. Actors automatically
// get this context, non-actors using mailboxes bound
// to this server should monitor this context to know
// when the server is trying to exit.
func (s *Server) Context() context.Context {
	return s.ctx
}

// Serve the grid on the listener. The listener address type must be
// net.TCPAddr, otherwise an error will be returned.
func (s *Server) Serve(lis net.Listener) error {
	// Create a registry client, through which other
	// entities like peers, actors, and mailboxes
	// will be discovered.
	r, err := registry.New(s.etcd)
	if err != nil {
		return err
	}
	s.registry = r
	s.registry.Timeout = s.cfg.Timeout
	s.registry.LeaseDuration = s.cfg.LeaseDuration

	// Set registry logger.
	if s.cfg.Logger != nil {
		r.Logger = s.cfg.Logger
	}

	// Create a context that each actor this leader creates
	// will receive. When the server is stopped, it will
	// call the cancel function, which should cause all the
	// actors it is responsible for to shutdown.
	ctx, cancel := context.WithCancel(context.Background())
	ctx = context.WithValue(ctx, contextKey, &contextVal{
		server: s,
	})
	s.ctx = ctx
	s.cancel = cancel

	// Start the registry and monitor that it is
	// running correctly.
	err = s.monitorRegistry(lis.Addr())
	if err != nil {
		return err
	}

	// Peer's name is the registry's name.
	name := s.registry.Registry()

	// Namespaced name, which just includes the namespace.
	nsName, err := namespaceName(Peers, s.cfg.Namespace, name)
	if err != nil {
		return err
	}

	// Register the namespace name, other peers can search
	// for this to discover each other.
	timeoutC, cancel := context.WithTimeout(ctx, s.cfg.Timeout)
	err = s.registry.Register(timeoutC, nsName)
	cancel()
	if err != nil {
		return err
	}

	// Create the mailboxes map.
	s.mu.Lock()
	s.mailboxes = make(map[string]*Mailbox)
	s.mu.Unlock()

	// Start a mailbox, this is critical because starting
	// actors in a grid is just done via a normal request
	// sending the message ActorDef to a listening peer's
	// mailbox.
	mailbox, err := NewMailbox(s, name, 100)
	if err != nil {
		return err
	}
	go s.runMailbox(mailbox)

	// Start the leader actor, and monitor, ie: make sure
	// that it's running.
	s.monitorLeader()

	// Monitor for fatal errors.
	s.monitorFatalErrors()

	// gRPC dance to start the gRPC server. The Serve
	// method blocks still stopped via a call to Stop.
	RegisterWireServer(s.grpc, s)
	err = s.grpc.Serve(lis)
	// Something in gRPC returns the "use of..." error
	// message even though it stopped fine. Catch that
	// error and don't pass it up.
	if err != nil && !strings.Contains(err.Error(), "use of closed network connection") {
		return err
	}

	// Return the final error state, which
	// could be set by other locations.
	return s.getFinalErr()
}

// Stop the server, blocking until all mailboxes registered with
// this server have called their close method.
func (s *Server) Stop() {
	logMailboxes := func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		for _, mailbox := range s.mailboxes {
			s.logf("%v: waiting for mailbox to close: %v", s.cfg.Namespace, mailbox)
		}
	}

	zeroMailboxes := func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return len(s.mailboxes) == 0
	}

	s.stop.Do(func() {
		if s.cancel == nil {
			return
		}
		s.cancel()

		t0 := time.Now()
		for {
			time.Sleep(200 * time.Millisecond)
			if zeroMailboxes() {
				break
			}
			if time.Now().Sub(t0) > 20*time.Second {
				t0 = time.Now()
				logMailboxes()
			}
		}

		s.registry.Stop()
		s.grpc.Stop()
	})
}

// Process a request and return a response. Implements the interface for
// gRPC definition of the wire service. Consider this a private method.
func (s *Server) Process(c netcontext.Context, d *Delivery) (*Delivery, error) {
	getMailbox := func() (*Mailbox, bool) {
		s.mu.Lock()
		defer s.mu.Unlock()
		m, ok := s.mailboxes[d.Receiver]
		return m, ok
	}

	mailbox, ok := getMailbox()
	if !ok {
		return nil, ErrUnknownMailbox
	}

	// Decode the request into an actual msg.
	msg, err := codec.Unmarshal(d.Data, d.TypeName)
	if err != nil {
		return nil, err
	}

	req := newRequest(c, msg)

	// Send the filled envelope to the actual
	// receiver. Also note that the receiver
	// can stop listenting when it wants, so
	// some defualt or timeout always needs
	// to exist here.
	select {
	case mailbox.c <- req:
	default:
		return nil, ErrReceiverBusy
	}

	// Wait for the receiver to send back a
	// reply, or the context to finish.
	select {
	case <-c.Done():
		return nil, ErrContextFinished
	case fail := <-req.failure:
		return nil, fail
	case res := <-req.response:
		return res, nil
	}
}

// runMailbox for this server.
func (s *Server) runMailbox(mailbox *Mailbox) {
	defer mailbox.Close()
	for {
		select {
		case <-s.ctx.Done():
			return
		case req := <-mailbox.C:
			switch msg := req.Msg().(type) {
			case *ActorStart:
				err := s.startActorC(req.Context(), msg)
				if err != nil {
					err2 := req.Respond(err)
					if err != nil {
						s.logf("%v: error sending respond(err:%v): err:%v", s.cfg.Namespace, err, err2)
					}
				} else {
					err := req.Ack()
					if err != nil {
						s.logf("%v: error sending ack: err:%v", s.cfg.Namespace, err)
					}
				}
			}
		}
	}
}

// monitorFatalErrors and stop the server if one occurs.
func (s *Server) monitorFatalErrors() {
	go func() {
		for {
			select {
			case <-s.ctx.Done():
				return
			case err := <-s.fatalErr:
				if err != nil {
					s.putFinalErr(err)
					s.Stop()
				}
			}
		}
	}()
}

// monitorRegistry for errors in the background.
func (s *Server) monitorRegistry(addr net.Addr) error {
	regFaults, err := s.registry.Start(addr)
	if err != nil {
		return err
	}
	go func() {
		select {
		case <-s.ctx.Done():
			return
		case err := <-regFaults:
			s.reportFatalError(err)
		}
	}()
	return nil
}

// monitorLeader starts a leader and keeps tyring to start
// a leader thereafter. If the leader should die on any
// host then some peer will eventually have it start again.
func (s *Server) monitorLeader() {
	startLeader := func() error {
		var err error
		for i := 0; i < 6; i++ {
			select {
			case <-s.ctx.Done():
				return nil
			default:
			}
			time.Sleep(1 * time.Second)
			err = s.startActor(s.cfg.Timeout, &ActorStart{Name: "leader", Type: "leader"})
			if err != nil && strings.Contains(err.Error(), registry.ErrAlreadyRegistered.Error()) {
				return nil
			}
		}
		return err
	}

	go func() {
		timer := time.NewTimer(0 * time.Second)
		defer timer.Stop()
		for {
			select {
			case <-s.ctx.Done():
				return
			case <-timer.C:
				err := startLeader()
				if err == ErrDefNotRegistered {
					s.logf("skipping leader startup since leader definition not registered")
					return
				}
				if err == ErrNilActor {
					s.logf("skipping leader startup since make leader returned nil")
					return
				}
				if err != nil {
					s.reportFatalError(fmt.Errorf("leader start failed: %v", err))
				} else {
					timer.Reset(30 * time.Second)
				}
			}
		}
	}()
}

// reportFatalError to the fatal error monitor. The
// consequence of a fatal error is handled by the
// monitor itself.
func (s *Server) reportFatalError(err error) {
	if err != nil {
		select {
		case s.fatalErr <- err:
		default:
		}
	}
}

func (s *Server) getFinalErr() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.finalErr
}

func (s *Server) putFinalErr(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.finalErr = err
}

// startActor in the current process. This method does not communicate with another
// system to choose where to run the actor. Calling this method will start the
// actor on the current host in the current process.
func (s *Server) startActor(timeout time.Duration, start *ActorStart) error {
	timeoutC, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return s.startActorC(timeoutC, start)
}

// startActorC in the current process. This method does not communicate with another
// system to choose where to run the actor. Calling this method will start the
// actor on the current host in the current process.
func (s *Server) startActorC(c context.Context, start *ActorStart) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !isNameValid(start.Type) {
		return ErrInvalidActorType
	}
	if !isNameValid(start.Name) {
		return ErrInvalidActorName
	}

	nsName, err := namespaceName(Actors, s.cfg.Namespace, start.Name)
	if err != nil {
		return err
	}

	makeActor := s.actors[start.Type]
	if makeActor == nil {
		return ErrDefNotRegistered
	}
	actor, err := makeActor(start.Data)
	if err != nil {
		return err
	}
	if actor == nil {
		return ErrNilActor
	}

	// Register the actor. This acts as a distributed mutex to
	// prevent an actor from starting twice on one system or
	// many systems.
	timeout, cancel := context.WithTimeout(c, s.cfg.Timeout)
	err = s.registry.Register(timeout, nsName)
	cancel()
	if err != nil {
		return err
	}

	// The actor's context contains its full id, it's name and the
	// full registration, which contains the actor's namespace.
	actorCtx := context.WithValue(s.ctx, contextKey, &contextVal{
		server:    s,
		actorID:   nsName,
		actorName: start.Name,
	})

	// Start the actor, unregister the actor in case of failure
	// and capture panics that the actor raises.
	go func() {
		defer func() {
			timeout, cancel := context.WithTimeout(context.Background(), s.cfg.Timeout)
			s.registry.Deregister(timeout, nsName)
			cancel()
		}()
		defer func() {
			if err := recover(); err != nil {
				stack := niceStack(debug.Stack())
				s.logf("panic in namespace: %v, actor: %v, recovered from: %v, stack trace: %v",
					s.cfg.Namespace, start.Name, err, stack)
			}
		}()
		actor.Act(actorCtx)
	}()

	return nil
}

func (s *Server) logf(format string, v ...interface{}) {
	if s.cfg.Logger != nil {
		s.cfg.Logger.Printf(format, v...)
	}
}
