package models

import (
	"strings"
	"sync"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/expr"
)

var (
	_ = u.EMPTY

	sourceMu        sync.Mutex
	sourceProviders = make(map[string]DataSourceCreator)
)

// A backend data source provider that also provides schema
type DataSource interface {
	datasource.DataSource
	Table(table string) (*Table, error)

	// Get a Task for given expression
	SourceTask(stmt *expr.SqlSelect) (SourceTask, error)
}

type SourceTask interface {
	//exec.TaskRunner
	datasource.Scanner
}

type DataSourceCreator func(*SourceSchema, *Config) DataSource

func DataSourceRegister(sourceType string, fn DataSourceCreator) {
	sourceMu.Lock()
	defer sourceMu.Unlock()
	sourceProviders[strings.ToLower(sourceType)] = fn
}

func DataSourceCreatorGet(sourceType string) DataSourceCreator {
	return sourceProviders[strings.ToLower(sourceType)]
}
