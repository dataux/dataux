package cloudstorage

import (
	"sort"
)

// Filter func type definition for filtering objects
type Filter func(objects Objects) Objects

// Query used to query the cloud source. The primary query is a prefix query like
// `ls /my-csv-files/baseball/*`.  This is the Request, and includes the
// PageSize, cursor/next token as well.
type Query struct {
	Delimiter  string   // Delimiter is most likely "/"
	Prefix     string   // prefix (directory) to search for or object name if one file
	Marker     string   // Next Page Marker if provided is a start next page fetch bookmark.
	ShowHidden bool     // Show hidden files?
	Filters    []Filter // Applied to the result sets to filter out Objects (i.e. remove objects by extension)
	PageSize   int      // PageSize defaults to global, or you can supply an override
}

// NewQuery create a query for finding files under given prefix.
func NewQuery(prefix string) Query {
	return Query{
		Prefix: prefix,
	}
}

// NewQueryAll query for all objects/files.
func NewQueryAll() Query {
	return Query{}
}

// NewQueryForFolders create a query for finding Folders under given path.
func NewQueryForFolders(folderPath string) Query {
	return Query{
		Delimiter: "/",
		Prefix:    folderPath,
	}
}

// AddFilter adds a post prefix query, that can be used to alter results set
// from the prefix query.
func (q *Query) AddFilter(f Filter) *Query {
	if q.Filters == nil {
		q.Filters = make([]Filter, 0)
	}
	q.Filters = append(q.Filters, f)
	return q
}

// Sorted added a sort Filter to the filter chain, if its not the last call
// while building your query, Then sorting is only guaranteed for the next
// filter in the chain.
func (q *Query) Sorted() *Query {
	q.AddFilter(ObjectSortFilter)
	return q
}

// ApplyFilters is called as the last step in store.List() to filter out the
// results before they are returned.
func (q *Query) ApplyFilters(objects Objects) Objects {
	for _, f := range q.Filters {
		objects = f(objects)
	}
	return objects
}

var ObjectSortFilter = func(objs Objects) Objects {
	sort.Stable(objs)
	return objs
}
