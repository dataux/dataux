grid
====

Grid is a library for doing distributed processing. It's main goal is to help
in scheduling fine-grain stateful computations, which grid calls actors, and
sending data between them. Its only service dependency is an
[Etcd v3](https://github.com/coreos/etcd) server, used for discovery and
coordination. Grid uses [gRPC](http://www.grpc.io/) for communication, and
sends [Protobuf](https://developers.google.com/protocol-buffers/)
messages.

## Example
Below is a basic example of starting your grid application. If a "leader"
definition is registered, the leader actor will be started for you when
`Serve` is called. The "leader" actor can be thought of as an entry-point
into you distributed application. You don't have to use it, but it is often
convenient. 

No matter how many processes are participating in the grid, only one leader
actor is started per namespace, it is a singleton.  The actor named "leader"
is also special in that if the process currently running the leader dies,
it will be started on another peer, if more than one peer is participating
in the grid.

```go
func main() {
    etcd, err := etcdv3.New(...)
    ...

    server, err := grid.NewServer(etcd, grid.ServerCfg{Namespace: "mygrid"})
    ...

    server.RegisterDef("leader", func(_ []byte) (grid.Actor, error) { return &LeaderActor{...}, nil })
    server.RegisterDef("worker", func(_ []byte) (grid.Actor, error) { return &WorkerActor{...}, nil })

    lis, err := net.Listen("tcp", ...)
    ...

    err = server.Serve(lis)
    ...
}

```

## Actor
Anything that implements the `Actor` interface is an actor. Actors typically
represent the central work of you application.

```go
type Actor interface {
    Act(ctx context.Context)
}
```

## Example Actor, Part 1
Below is an actor that starts other actors, this is a typical way of structuring
an application with grid. Here the leader actor starts a worker on each peer in
the grid. Actors are started by sending an `ActorStart` message to a peer.
Each actor must have a unique name, per namespace. The name is registered in Etcd
to make sure that it is unique across all the processes of a grid.

```go
const timeout = 2 * time.Second

type LeaderActor struct {
    client *grid.Client
}

func (a *LeaderActor) Act(ctx context.Context) {
    // Discover participating peers.
    peers, err := a.client.Query(timeout, grid.Peers)
    ...

    i := 0
    for _, peer := range peers {
        // Actor names are unique, registered in etcd.
        // There can never be more than one actor with
        // a given name. When an actor exits or panics
        // its record is removed from etcd.
        start := grid.NewActorStart("worker-%d", i)
        start.Type = "worker"

        // Start a new actor on the given peer. The message
        // "ActorStart" is special. When sent to the mailbox
        // of a peer, that peer will start an actor based on
        // the definition.
        res, err := a.client.Request(timeout, peer.Name(), start)
        ...
        i++
    }

    ...
}
```

## Example Actor, Part 2
An actor will typically need to receive data to work on. This may come
from the filesystem or a database, but it can also come from messages
sent to a mailbox. Just like actors, a mailbox is unique by name. Etcd
is used to register the name and guarantee that only one such mailbox
exists.

```go
const size = 10

type WorkerActor struct {
    server *grid.Server
}

func (a *WorkerActor) Act(ctx context.Context) {
    name, err := grid.ContextActorName(ctx)
    ...

    // Listen to a mailbox with the same
    // name as the actor.
    mailbox, err := grid.NewMailbox(a.server, name, size)
    ...
    defer mailbox.Close()

    for {
        select {
        case req := <-mailbox.C:
            switch req.Msg().(type) {
            case PingMsg:
                err := req.Respond(&PongMsg{
                    ...
                })
        }
    }
}
```

## Example Actor, Part 3
Each actor receives a context as a parameter in its `Act` method. That context
is created by the peer that started the actor. The context contains several
useful values, they can be extracted using the `Context*` functions.

```go
func (a *WorkerActor) Act(ctx context.Context) {
    // The ID of the actor in etcd.
    id, err := grid.ContextActorID(ctx)

    // The name of the actor, as given in ActorStart.
    name, err := grid.ContextActorName(ctx)

    // The namespace of the grid this actor is associated with.
    namespace, err := grid.ContextActorNamespace(ctx)
}
```

## Example Actor, Part 4
An actor can exit whenever it wants, but it *must* exit when its context
signals done. An actor should always monitor its context Done channel.

```go
func (a *WorkerActor) Act(ctx context.Context) {
    for {
        select {
        case <-ctx.Done():
            // Stop requested, clean up and exit.
            return
        case ...
        }
    }
}
```

## Example Actor, Part 5
Each actor is registered into etcd. Consequently each actor's name acts like
a mutex. If code requests the actor to start *twice* the second request will
receive an error indicating that the actor is already registered.

```go
const timeout = 2 * time.Second

type LeaderActor struct {
    client *grid.Client
}

func (a *LeaderActor) Act(ctx context.Context) {
    start := grid.NewActorStart("worker-%d", 0)
    start.Type = "worker"

    // First request to start.
    err := a.client.Request(timeout, peer, start)

    // Second request will fail, if the first succeeded.
    err = a.client.Request(timeout, peer, start)
}
```

## Kubernetes + Grid
The examples above are meant to give some intuitive sense of what the grid
library does. Howevery what it does not do is:

 1. Package up your congifuration and binaries
 1. Start your VMs
 1. Start your processes on those VMs
 1. Autoscale your VMs when resources run low
 1. Reschedule crashed processes
 1. etc...

This is intentional as other tools already do these things. At the top of
our list is Kubernetes and Docker, which between the two perform all of the
above.

Grid comes into the picture once you start building out your application logic
and need things like coordination and messaging, which under the hood in grid
is done with Etcd and gRPC - taking care of some boilerplate code for you.

## Sending Messages
Sending messages is always done through the client. The client configuration
has only one required parameter, the namespace of the grid to connect to.
Different namespaces can communicate by simply creating clients to the
namespace they wish to send messages.

```go
const timeout = 2 * time.Second


func Example() {
    etcd, err := etcdv3.New(...)
    ...

    client, err := grid.NewClient(etcd, grid.ClientCfg{Namespace: "myapp"})
    ...

    res, err := client.Request(timeout, "some-mailbox-name", &MyMsg{
        ...
    })

    ... process the response ...
}
```

### Registering Messages
Every type of message must be registered before use. Each message must be a
Protobuf message. See the [Go Protobuf Tutorial](https://developers.google.com/protocol-buffers/docs/gotutorial)
for more information, or the example below:

```protobuf
syntax = "proto3";
package msg;

message Person {
    string name = 1;
    string email = 2;
    ...
}
```

Before using the message it needs to be registered, which can be done
inside init functions, the main function, or just before first sending
and receiving the message.

```go
func main() {
    grid.Register(msg.Person{})

    ...
}
```