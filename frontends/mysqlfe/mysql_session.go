package mysqlfe

import (
	"time"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
)

// http://dev.mysql.com/doc/refman/5.6/en/server-system-variables.html
var mysqlGlobalVars *datasource.ContextSimple = NewMySqlGlobalVars()

func NewMySqlSessionVars(db, user string, connId uint32) expr.ContextReadWriter {
	ctx := datasource.NewContextSimple()
	ctx.Data["@@dataux.dialect"] = value.NewStringValue("mysql")
	ctx.Data["@@database"] = value.NewStringValue(db)
	ctx.Data["@@user"] = value.NewStringValue(user)
	ctx.Data["@@session.tx_isolation"] = value.NewStringValue("REPEATABLE-READ")
	ctx.Data["@@connection_id"] = value.NewIntValue(int64(connId))
	ctx.Data["@@max_allowed_packet"] = value.NewIntValue(MaxAllowedPacket)
	ctx.Data["@@session.auto_increment_increment"] = value.NewIntValue(1)

	// ctx.Data["@@connection_id"] = value.NewStringValue(fmt.Sprintf("%v", connId))
	// ctx.Data["@@max_allowed_packet"] = value.NewStringValue(MaxAllowedPacketStr)
	// ctx.Data["@@session.auto_increment_increment"] = value.NewStringValue("1")

	rw := datasource.NewNestedContextReadWriter([]expr.ContextReader{
		ctx,
		mysqlGlobalVars,
	}, ctx, time.Now())
	return rw
}

/*
ql> show variables like '%timeout';
+----------------------------+-------+
| Variable_name              | Value |
+----------------------------+-------+
| connect_timeout            | 10    |
| delayed_insert_timeout     | 300   |
| innodb_lock_wait_timeout   | 50    |
| innodb_rollback_on_timeout | OFF   |
| interactive_timeout        | 60    |
| net_read_timeout           | 30    |
| net_write_timeout          | 60    |
| slave_net_timeout          | 3600  |
| table_lock_wait_timeout    | 50    |
| wait_timeout               | 60    |
+----------------------------+-------+
*/

func NewMySqlGlobalVars() *datasource.ContextSimple {
	ctx := datasource.NewContextSimple()

	ctx.Data["@@session.auto_increment_increment"] = value.NewIntValue(1)
	ctx.Data["@@session.tx_read_only"] = value.NewIntValue(1)
	ctx.Data["@@interactive_timeout"] = value.NewIntValue(28800)
	ctx.Data["@@lower_case_table_names"] = value.NewIntValue(0)
	ctx.Data["@@max_allowed_packet"] = value.NewIntValue(MaxAllowedPacket)
	ctx.Data["@@max_allowed_packets"] = value.NewIntValue(MaxAllowedPacket)
	ctx.Data["@@net_buffer_length"] = value.NewIntValue(16384)
	ctx.Data["@@net_write_timeout"] = value.NewIntValue(600)
	ctx.Data["@@query_cache_size"] = value.NewIntValue(1048576)
	ctx.Data["@@wait_timeout"] = value.NewIntValue(28800)
	ctx.Data["@@character_set_client"] = value.NewStringValue("utf8")
	ctx.Data["@@character_set_connection"] = value.NewStringValue("utf8")
	ctx.Data["@@character_set_results"] = value.NewStringValue("utf8")
	ctx.Data["@@character_set_server"] = value.NewStringValue("utf8")
	ctx.Data["@@init_connect"] = value.NewStringValue("")
	ctx.Data["@@license"] = value.NewStringValue("MIT")
	ctx.Data["@@query_cache_type"] = value.NewStringValue("OFF")
	ctx.Data["@@sql_mode"] = value.NewStringValue("NO_ENGINE_SUBSTITUTION")
	ctx.Data["@@system_time_zone"] = value.NewStringValue("UTC")
	ctx.Data["@@time_zone"] = value.NewStringValue("SYSTEM")
	ctx.Data["@@tx_isolation"] = value.NewStringValue("REPEATABLE-READ")
	ctx.Data["@@version_comment"] = value.NewStringValue("DataUX (MIT), Release .13")
	ctx.Data["@@character_set_server"] = value.NewStringValue("utf8")
	return ctx
}
