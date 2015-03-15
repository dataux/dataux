package models

import (
	u "github.com/araddon/gou"
	"strings"
	"sync"
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
	//Schema(db string) (*Schema, error)
	Table(table string) (*Table, error)
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
