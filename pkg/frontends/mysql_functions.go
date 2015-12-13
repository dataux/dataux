package frontends

import (
	"sync"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
)

var _ = u.EMPTY
var loadOnce sync.Once

func init() {
	LoadMySqlFunctions()
}
func LoadMySqlFunctions() {
	loadOnce.Do(func() {
		expr.FuncAdd("current_user", CurrentUser)
		expr.FuncAdd("connection_id", ConnectionId)
	})
}

// ConnectionId:   id of current connection
//
//      connection_id()     =>  11, true
//
func ConnectionId(ctx expr.EvalContext) (value.IntValue, bool) {
	// switch node := val.(type) {
	// case value.StringValue:
	// 	return value.NewIntValue(int64(len(node.Val()))), true
	// }
	u.Infof("ConnectionId: %#v", ctx)
	return value.NewIntValue(1), true
}

// CurrentUser:   username of current user
//
//      current_user()     =>  user, true
//
func CurrentUser(ctx expr.EvalContext) (value.StringValue, bool) {
	// switch node := val.(type) {
	// case value.StringValue:
	// 	return value.NewIntValue(int64(len(node.Val()))), true
	// }
	u.Infof("CurrentUser: %#v", ctx)
	return value.NewStringValue("root"), true
}
