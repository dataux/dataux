package models

import (
	"strings"
	"sync"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/expr"
)

var (
	_ = u.EMPTY

	sourceMu        sync.Mutex
	sourceProviders = make(map[string]DataSourceCreator)
)

// A backend data source provider
type DataSource interface {
	Init() error
	Close() error
	Features() *datasource.SourceFeatures
	Table(table string) (*Table, error)
	SourceTask(stmt *expr.SqlSelect) (SourceTask, error)
}

type SourceTask interface {
	exec.TaskRunner
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
