package frontends

import (
	"github.com/araddon/qlbridge/datasource"
)

func NewMysqlSession() *datasource.ContextSimple {
	ctx := datasource.NewContextSimple()
	return ctx
}
