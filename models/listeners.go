package models

import (
	"strings"
	"sync"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/schema"
)

var (
	listenerMu sync.Mutex
	listeners  = make(map[string]Listener)
)

type ResultWriter interface {
	WriteResult(Result) error
}

type Result interface{}

type StatementHandlerCreator interface {
	Open(conn interface{}) StatementHandler
}

// A listener is a protocol specific, and transport specific
//  reader of requests which will be routed to a handler
type Listener interface {
	Init(*ListenerConfig, *ServerCtx) error
	Run(stop chan bool) error
	Close() error
}

// A statement handler fulfills a frontend network client request.
// Examples of handlers are mysql, mongo, etc
type StatementHandler interface {
	SchemaUse(db string) *schema.Schema
	Handle(writer ResultWriter, req *Request) error
	Close() error
}

// a DataUx Request contains the request/command
// from client and references to session and schema
type Request struct {
	Raw     []byte // raw full byte statement
	Db      string // Db name parsed from statement
	Schema  *schema.Schema
	Session expr.ContextReader
}

func ListenerRegister(name string, l Listener) {
	listenerMu.Lock()
	defer listenerMu.Unlock()
	listeners[strings.ToLower(name)] = l
}

func Listeners() map[string]Listener {
	return listeners
}

func ListenerGet(name string) Listener {
	return listeners[strings.ToLower(name)]
}
