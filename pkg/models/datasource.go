package models

import (
	"strings"
	"sync"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/datasource"
	//"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/expr"
)

var (
	_ = u.EMPTY

	sourceMu        sync.Mutex
	sourceProviders = make(map[string]DataSourceCreator)
)

// A backend data source provider
type DataSource interface {
	// Backends may be persistent connections, so would require initilization
	Init() error
	Close() error

	// Describe the Features Available on this Datasource, so we know
	// which features to defer to db, which to implement in dataux
	Features() *datasource.SourceFeatures

	// ???
	Table(table string) (*Table, error)

	// Get a Task for given expression
	SourceTask(stmt *expr.SqlSelect) (SourceTask, error)
}

type SourceTask interface {
	//exec.TaskRunner
	datasource.Scanner
}

// Some data sources that implement more features, can provide
//  their own projection.
type SourceProjection interface {
	// Describe the Columns etc
	Projection() (*expr.Projection, error)
}

type DataSourceCreator func(*Schema, *Config) DataSource

func DataSourceRegister(sourceType string, fn DataSourceCreator) {
	sourceMu.Lock()
	defer sourceMu.Unlock()
	sourceProviders[strings.ToLower(sourceType)] = fn
}

func DataSourceCreatorGet(sourceType string) DataSourceCreator {
	return sourceProviders[strings.ToLower(sourceType)]
}
