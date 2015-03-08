package elasticsearch

import (
	//"fmt"
	//"strings"

	//"database/sql"
	"database/sql/driver"
	u "github.com/araddon/gou"
)

var (
	_ driver.Rows = (*Response)(nil)
)

/*

-- Database Driver Rows

type Rows interface {
        // Columns returns the names of the columns. The number of
        // columns of the result is inferred from the length of the
        // slice.  If a particular column name isn't known, an empty
        // string should be returned for that entry.
        Columns() []string

        // Close closes the rows iterator.
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
        Next(dest []Value) error
}
*/

// Responsible for Translating the Elasticsearch Json response
//  To a Mysql(??) Response
type Response struct {
	cols     []string
	Docs     []u.JsonHelper
	Total    int
	Aggs     u.JsonHelper
	ScrollId string
	Request  *SqlToEs
}

func (m *Response) Close() error { return nil }

func (m *Response) Columns() []string { return m.cols }

// Next is called to populate the next row of data into
// the provided slice. The provided slice will be the same
// size as the Columns() are wide.
//
// The dest slice may be populated only with
// a driver Value type, but excluding string.
// All string values must be converted to []byte.
//
// Next should return io.EOF when there are no more rows.
func (m *Response) Next(dest []driver.Value) error {
	return nil
}
