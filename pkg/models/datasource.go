package models

import (
	"strings"
	"sync"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/datasource"
)

var (
	_ = u.EMPTY

	sourceMu        sync.Mutex
	sourceProviders = make(map[string]DataSourceCreator)
)

// A backend data source provider that also provides schema
type DataSource interface {
	datasource.DataSource
}

type DataSourceCreator func(*datasource.SourceSchema, *Config) DataSource

func DataSourceRegister(sourceType string, fn DataSourceCreator) {
	sourceMu.Lock()
	defer sourceMu.Unlock()
	//u.LogTracef(u.WARN, "hello")
	sourceProviders[strings.ToLower(sourceType)] = fn
}

func DataSourceCreatorGet(sourceType string) DataSourceCreator {
	return sourceProviders[strings.ToLower(sourceType)]
}
