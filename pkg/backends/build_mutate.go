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

	u.Debugf("VisitInsert %s", stmt)
	//u.Debugf("VisitInsert %T  %s\n%#v", stmt, stmt.String(), stmt)
	tasks := make(exec.Tasks, 0)

	tableName := strings.ToLower(stmt.Table)
	tbl, err := m.schema.Table(tableName)
	if err != nil {
		u.Warnf("error finding table %v", err)
		return nil, err
	}

	features := tbl.SourceSchema.DSFeatures.Features
	if features.SourceMutation {
		source, err := tbl.SourceSchema.DS.(datasource.SourceMutation).Create(tbl, stmt)
		if err != nil {
			u.Warnf("error finding table %v", err)
			return nil, err
		}
		insertTask := exec.NewInsertUpsert(stmt, source)
		//u.Debugf("adding insert source %#v", source)
		//u.Infof("adding insert: %#v", insertTask)
		tasks.Add(insertTask)
	} else if features.Upsert {
		source := tbl.SourceSchema.DS.(datasource.Upsert)
		insertTask := exec.NewInsertUpsert(stmt, source)
		u.Debugf("adding insert source %#v", source)
		u.Infof("adding insert: %#v", insertTask)
		tasks.Add(insertTask)
	} else {
		return nil, fmt.Errorf("%T Must Implement Upsert or SourceMutation", tbl.SourceSchema.DS)
	}

	return tasks, nil
}

func (m *Builder) VisitUpdate(stmt *expr.SqlUpdate) (interface{}, error) {
	u.Debugf("VisitUpdate %+v", stmt)
	//u.Debugf("VisitUpdate %T  %s\n%#v", stmt, stmt.String(), stmt)
	tasks := make(exec.Tasks, 0)

	tableName := strings.ToLower(stmt.Table)
	tbl, err := m.schema.Table(tableName)
	if err != nil {
		u.Warnf("error finding table %v", err)
		return nil, err
	}

	features := tbl.SourceSchema.DSFeatures.Features
	if features.SourceMutation {
		source, err := tbl.SourceSchema.DS.(datasource.SourceMutation).Create(tbl, stmt)
		if err != nil {
			u.Warnf("error finding table %v", err)
			return nil, err
		}
		task := exec.NewUpdateUpsert(stmt, source)
		//u.Debugf("adding update source %#v", source)
		//u.Infof("adding update: %#v", task)
		tasks.Add(task)
	} else if features.Upsert {
		source := tbl.SourceSchema.DS.(datasource.Upsert)
		task := exec.NewUpdateUpsert(stmt, source)
		u.Debugf("adding update source %#v", source)
		u.Infof("addinng update: %#v", task)
		tasks.Add(task)
	} else {
		return nil, fmt.Errorf("%T Must Implement Upsert or SourceMutation", tbl.SourceSchema.DS)
	}

	return tasks, nil
}

func (m *Builder) VisitUpsert(stmt *expr.SqlUpsert) (interface{}, error) {
	u.Debugf("VisitUpsert %+v", stmt)
	return nil, ErrNotImplemented
}

func (m *Builder) VisitDelete(stmt *expr.SqlDelete) (interface{}, error) {
	u.Debugf("VisitDelete %+v", stmt)
	tasks := make(exec.Tasks, 0)
	tbl, err := m.schema.Table(strings.ToLower(stmt.Table))
	if err != nil {
		u.Warnf("error finding table %v", err)
		return nil, err
	}

	features := tbl.SourceSchema.DSFeatures.Features
	if features.SourceMutation {
		source, err := tbl.SourceSchema.DS.(datasource.SourceMutation).Create(tbl, stmt)
		if err != nil {
			u.Warnf("error finding table %v", err)
			return nil, err
		}
		task := exec.NewDelete(stmt, source)
		//u.Debugf("adding delete source %#v", source)
		//u.Infof("adding delete: %#v", task)
		tasks.Add(task)
	} else if features.Deletion {
		source := tbl.SourceSchema.DS.(datasource.Deletion)
		task := exec.NewDelete(stmt, source)
		u.Debugf("adding delete source %#v", source)
		u.Infof("adding delete: %#v", task)
		tasks.Add(task)
	} else {
		return nil, fmt.Errorf("%T Must Implement Deletion or SourceMutation", tbl.SourceSchema.DS)
	}

	return tasks, nil
}
