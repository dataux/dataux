package models

import (
	"database/sql/driver"

	//u "github.com/araddon/gou"
	//"github.com/araddon/qlbridge/expr"
	//"github.com/araddon/qlbridge/value"
)

// type ResultSession interface {
// 	ResultProvider() ResultProvider
// }

type ResultWriter interface {
	WriteResult(Result) error
}

type Result interface{}

// ResultProvider is a Result Interface for bridging between backends/frontends
//  - Next() same as database/sql driver interface for populating values
//  - schema, we need col/data-types to write headers, typed interfaces
type ResultProvider interface {
	// The underlying schema
	//Schema() *Schema
	// Columns returns a definition for columns in this result
	//Columns() []*ResultColumn

	// Close closes the result provider
	Close() error

	// Next is called to populate the next row of data into
	// the provided slice. The provided slice will be the same
	// size as the Columns() are wide.
	//
	// The dest slice may be populated only with
	// a driver Value type, but excluding string.
	// All string values must be converted to []byte.
	//
	// Next should return io.EOF when there are no more rows.
	Next(dest []driver.Value) error
}

// type ValsMessage struct {
// 	Vals []driver.Value
// 	Id   uint64
// }

// func (m ValsMessage) Key() uint64       { return m.Id }
// func (m ValsMessage) Body() interface{} { return m.Vals }

// type ResultColumn struct {
// 	Name   string          // Original path/name for query field
// 	Pos    int             // Ordinal position in sql statement
// 	SqlCol *expr.Column    // the original sql column
// 	Type   value.ValueType // Type
// }
