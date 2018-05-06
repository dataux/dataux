package grid

import "errors"

var (
	// ErrInvalidName when name contains invalid character codes.
	ErrInvalidName = errors.New("grid: invalid name")
	// ErrInvalidNamespace when namespace contains invalid
	// character codes.
	ErrInvalidNamespace = errors.New("grid: invalid namespace")
	// ErrInvalidActorType when the actor type contains invalid
	// character codes.
	ErrInvalidActorType = errors.New("grid: invalid actor type")
	// ErrInvalidActorName when the actor name contains invalid
	// character codes.
	ErrInvalidActorName = errors.New("grid: invalid actor name")
	// ErrInvalidMailboxName when a mailbox name contains invalid
	// character codes.
	ErrInvalidMailboxName = errors.New("grid: invalid mailbox name")
)

var (
	// ErrReceiverBusy when the message buffer of a mailbox is
	// full, conisder a larger size when creating the mailbox.
	ErrReceiverBusy = errors.New("grid: receiver busy")
	// ErrUnknownMailbox when a message is received by a peer for
	// a mailbox the peer does not serve, likely the mailbox has
	// moved between the time of discovery and the message receive.
	ErrUnknownMailbox = errors.New("grid: unknown mailbox")
	// ErrUnregisteredMailbox when a mailbox name does not exist in
	// the registry, likely it was never created or has died.
	ErrUnregisteredMailbox = errors.New("grid: unregistered mailbox")
	// ErrContextFinished when the context signals done before the
	// request could receive a response from the receiver.
	ErrContextFinished = errors.New("grid: context finished")
)

var (
	// ErrNilEtcd when the etcd argument is nil.
	ErrNilEtcd = errors.New("grid: nil etcd")
	// ErrNilActor when an actor definition has been registered
	// but returns a nil actor and nil error when creating an actor.
	ErrNilActor = errors.New("grid: nil actor")
	// ErrInvalidContext when a context does not contain
	// the requested values.
	ErrInvalidContext = errors.New("grid: invalid context")
	// ErrDefNotRegistered when a actor type which has never
	// been registered is requested for start.
	ErrDefNotRegistered = errors.New("grid: def not registered")
	// ErrServerNotRunning when an operation which requires the
	// server be running, but is not, is requested.
	ErrServerNotRunning = errors.New("grid: server not running")
	// ErrAlreadyRegistered when a mailbox is created but someone
	// else has already created it.
	ErrAlreadyRegistered = errors.New("grid: already registered")
	// ErrWatchClosedUnexpectedly when a query watch closes before
	// it was requested to close, likely do to some etcd issue.
	ErrWatchClosedUnexpectedly = errors.New("grid: watch closed unexpectedly")
)
