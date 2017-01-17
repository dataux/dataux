package mysqlfe

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
		expr.FuncAdd("current_user", &funcTemplate{
			field:    "@@user",
			t:        value.StringType,
			defvalue: value.NewStringValue("root"),
		})
		expr.FuncAdd("connection_id", &funcTemplate{
			field:    "@@connection_id",
			t:        value.IntType,
			defvalue: value.NewIntValue(1),
		})
		expr.FuncAdd("database", &funcTemplate{
			field:    "@@database",
			t:        value.StringType,
			defvalue: value.NewStringValue(""),
		})
	})
}

// DatabaseName:   name of the database
//
//      DATABASE()     =>  "your_db", true
//

// ConnectionId:   id of current connection
//
//      connection_id()     =>  11, true
//

// CurrentUser:   username of current user
//
//      current_user()     =>  user, true
//

type funcTemplate struct {
	t        value.ValueType
	field    string
	defvalue value.Value
}

func (m *funcTemplate) Eval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {
	if ctx == nil {
		if m.defvalue != nil {
			return m.defvalue, true
		}
		return m.defvalue, false
	}
	v, ok := ctx.Get(m.field)
	if !ok {
		if m.defvalue != nil {
			return m.defvalue, true
		}
		return v, false
	}
	return v, true
}
func (m *funcTemplate) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	return m.Eval, nil
}
func (m *funcTemplate) Type() value.ValueType { return m.t }
