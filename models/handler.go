package models

import (
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/schema"
)

// a DataUx Request contains the request/command
// from client and references to session and schema
type Request struct {
	Raw     []byte // raw full byte statement
	Db      string // Db name parsed from statement
	Schema  *schema.Schema
	Session expr.ContextReader
}

// A request handler fulfills a frontend network client request.
// Examples of handlers are mysql, mongo, etc
type Handler interface {
	// Get and Set this db/schema for this persistent handler
	SchemaUse(db string) *schema.Schema
	Handle(writer ResultWriter, req *Request) error
	Close() error
}

// Some handlers implement the Session Specific interface
type ConnectionHandle interface {
	Open(conn interface{}) Handler
}
