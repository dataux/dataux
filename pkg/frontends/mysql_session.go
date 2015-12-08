package frontends

import (
	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/value"
)

func NewMysqlSession() *datasource.ContextSimple {
	ctx := datasource.NewContextSimple()
	ctx.Data["@@max_allowed_packet"] = value.NewIntValue(MaxAllowedPacket)
	return ctx
}
