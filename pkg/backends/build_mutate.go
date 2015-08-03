package backends

import (
	"fmt"
	"strings"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/expr"
)

var (
	_ = u.EMPTY
)

func (m *Builder) VisitInsert(stmt *expr.SqlInsert) (interface{}, error) {

	u.Debugf("VisitInsert %+v", stmt)
	tasks := make(exec.Tasks, 0)

	tableName := strings.ToLower(stmt.Table)
	tbl, err := m.schema.Table(tableName)
	if err != nil {
		return nil, err
	}

	// Must provider either Scanner, and or Seeker interfaces
	source, ok := tbl.SourceSchema.DS.(datasource.Upsert)
	if !ok {
		return nil, fmt.Errorf("%T Must Implement Upsert", tbl.SourceSchema.DS)
	}

	insertTask := exec.NewInsertUpsert(stmt, source)
	//u.Infof("adding insert: %#v", insertTask)
	tasks.Add(insertTask)

	return tasks, nil
}

func (m *Builder) VisitUpsert(stmt *expr.SqlUpsert) (interface{}, error) {
	u.Debugf("VisitUpsert %+v", stmt)
	return nil, ErrNotImplemented
}

func (m *Builder) VisitDelete(stmt *expr.SqlDelete) (interface{}, error) {
	u.Debugf("VisitDelete %+v", stmt)
	return nil, ErrNotImplemented
}

func (m *Builder) VisitUpdate(stmt *expr.SqlUpdate) (interface{}, error) {
	u.Debugf("VisitUpdate %+v", stmt)
	return nil, ErrNotImplemented
}
