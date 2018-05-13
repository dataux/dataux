package grid

import (
	"context"
	"strings"
)

// Mailbox for receiving messages.
type Mailbox struct {
	name    string
	nsName  string
	C       <-chan Request
	c       chan Request
	cleanup func() error
}

// Close the mailbox.
func (box *Mailbox) Close() error {
	return box.cleanup()
}

// Name of mailbox, without namespace.
func (box *Mailbox) Name() string {
	return box.name
}

// String of mailbox name, with full namespace.
func (box *Mailbox) String() string {
	return box.nsName
}

// NewMailbox for requests addressed to name. Size will be the mailbox's
// channel size.
//
// Example Usage:
//
//     mailbox, err := NewMailbox(server, "incoming", 10)
//     ...
//     defer mailbox.Close()
//
//     for {
//         select {
//         case req := <-mailbox.C:
//             // Do something with request, and then respond
//             // or ack. A response or ack is required.
//             switch m := req.Msg().(type) {
//             case HiMsg:
//                 req.Respond(&HelloMsg{})
//             }
//         }
//     }
//
// If the mailbox has already been created, in the calling process or
// any other process, an error is returned, since only one mailbox
// can claim a particular name.
//
// Using a mailbox requires that the process creating the mailbox also
// started a grid Server.
func NewMailbox(s *Server, name string, size int) (*Mailbox, error) {
	if !isNameValid(name) {
		return nil, ErrInvalidMailboxName
	}

	// Namespaced name.
	nsName, err := namespaceName(Mailboxes, s.cfg.Namespace, name)
	if err != nil {
		return nil, err
	}

	return newMailbox(s, name, nsName, size)
}

func newMailbox(s *Server, name, nsName string, size int) (*Mailbox, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.mailboxes == nil {
		return nil, ErrServerNotRunning
	}

	_, ok := s.mailboxes[nsName]
	if ok {
		return nil, ErrAlreadyRegistered
	}

	timeout, cancel := context.WithTimeout(context.Background(), s.cfg.Timeout)
	err := s.registry.Register(timeout, nsName)
	cancel()
	// Check if the error is a particular fatal error
	// from etcd. Some errors have no recovery. See
	// the list of all possible errors here:
	//
	// https://github.com/coreos/etcd/blob/master/etcdserver/api/v3rpc/rpctypes/error.go
	//
	// They are unfortunately not classidied into
	// recoverable or non-recoverable.
	if err != nil && strings.Contains(err.Error(), "etcdserver: requested lease not found") {
		s.reportFatalError(err)
		return nil, err
	}
	if err != nil {
		return nil, err
	}

	boxC := make(chan Request, size)
	cleanup := func() error {
		s.mu.Lock()
		defer s.mu.Unlock()

		close(boxC)

		// Immediately delete the subscription so that no one
		// can send to it, at least from this host.
		delete(s.mailboxes, nsName)

		// Deregister the name.
		timeout, cancel := context.WithTimeout(context.Background(), s.cfg.Timeout)
		defer cancel()
		err := s.registry.Deregister(timeout, nsName)

		// Return any error from the deregister call.
		return err
	}
	box := &Mailbox{
		name:    name,
		nsName:  nsName,
		C:       boxC,
		c:       boxC,
		cleanup: cleanup,
	}
	s.mailboxes[nsName] = box
	return box, nil
}
