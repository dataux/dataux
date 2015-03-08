package models

import (
	"github.com/dataux/dataux/vendor/mixer/sqlparser"
)

type Request struct {
	Raw    []byte              // raw full statement
	Stmt   sqlparser.Statement // do we really need statement here
	Db     string              // Db name parsed from statement
	Schema *Schema
}

type Handler interface {
	// Get and Set this db/schema for this persistent handler
	SchemaUse(db string) *Schema
	Handle(writer ResultWriter, req *Request) error
	Close() error
}

// Some handlers implement the Session Specific interface
type HandlerSession interface {
	Clone(conn interface{}) Handler
}

type ResultWriter interface {
	WriteResult(Result) error
}

type Result interface{}
