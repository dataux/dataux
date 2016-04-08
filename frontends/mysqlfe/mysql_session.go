package mysqlfe

import (
	"time"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
)

// http://dev.mysql.com/doc/refman/5.6/en/server-system-variables.html
var mysqlGlobalVars *datasource.ContextSimple = NewMySqlGlobalVars()

func NewMySqlSessionVars(db string, connId uint32) expr.ContextReadWriter {
	ctx := datasource.NewContextSimple()
	ctx.Data["@@dataux.dialect"] = value.NewStringValue("mysql")
	ctx.Data["@@database"] = value.NewStringValue(db)
	ctx.Data["@@connection_id"] = value.NewIntValue(int64(connId))
	ctx.Data["@@max_allowed_packet"] = value.NewIntValue(MaxAllowedPacket)
	ctx.Data["@@session.auto_increment_increment"] = value.NewIntValue(1)
	ctx.Data["@@session.tx_isolation"] = value.NewStringValue("REPEATABLE-READ")
	rdr := datasource.NewNestedContextReadWriter([]expr.ContextReader{
		ctx,
		mysqlGlobalVars,
	}, ctx, time.Now())
	return rdr
}

func NewMySqlGlobalVars() *datasource.ContextSimple {
	ctx := datasource.NewContextSimple()

	ctx.Data["@@session.auto_increment_increment"] = value.NewIntValue(1)
	ctx.Data["@@session.tx_read_only"] = value.NewIntValue(1)
	//ctx.Data["@@session.auto_increment_increment"] = value.NewBoolValue(true)
	ctx.Data["@@character_set_client"] = value.NewStringValue("utf8")
	ctx.Data["@@character_set_connection"] = value.NewStringValue("utf8")
	ctx.Data["@@character_set_results"] = value.NewStringValue("utf8")
	ctx.Data["@@character_set_server"] = value.NewStringValue("utf8")
	ctx.Data["@@init_connect"] = value.NewStringValue("")
	ctx.Data["@@interactive_timeout"] = value.NewIntValue(28800)
	ctx.Data["@@license"] = value.NewStringValue("MIT")
	ctx.Data["@@lower_case_table_names"] = value.NewIntValue(0)
	ctx.Data["@@max_allowed_packet"] = value.NewIntValue(MaxAllowedPacket)
	ctx.Data["@@max_allowed_packets"] = value.NewIntValue(MaxAllowedPacket)
	ctx.Data["@@net_buffer_length"] = value.NewIntValue(16384)
	ctx.Data["@@net_write_timeout"] = value.NewIntValue(600)
	ctx.Data["@@query_cache_size"] = value.NewIntValue(1048576)
	ctx.Data["@@query_cache_type"] = value.NewStringValue("OFF")
	ctx.Data["@@sql_mode"] = value.NewStringValue("NO_ENGINE_SUBSTITUTION")
	ctx.Data["@@system_time_zone"] = value.NewStringValue("UTC")
	ctx.Data["@@time_zone"] = value.NewStringValue("SYSTEM")
	ctx.Data["@@tx_isolation"] = value.NewStringValue("REPEATABLE-READ")
	ctx.Data["@@version_comment"] = value.NewStringValue("DataUX (MIT), Release .13")
	ctx.Data["@@wait_timeout"] = value.NewIntValue(28800)
	return ctx
}
