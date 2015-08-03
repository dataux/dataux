package backends

import (
	"fmt"
	"strings"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/datasource/inmemmap"
	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
)

func (m *Builder) VisitSelect(stmt *expr.SqlSelect) (interface{}, error) {

	if sysVar := stmt.SysVariable(); len(sysVar) > 0 {
		return m.VisitSysVariable(stmt)
	} else if len(stmt.From) == 0 && len(stmt.Columns) == 1 && strings.ToLower(stmt.Columns[0].As) == "database" {
		return m.VisitSelectDatabase(stmt)
	}

	u.Debugf("VisitSelect %+v", stmt)

	tasks := make(exec.Tasks, 0)

	if len(stmt.From) == 1 {
		from := stmt.From[0]
		tableName := strings.ToLower(from.Name)

		tbl, err := m.schema.Table(tableName)
		if err != nil {
			return nil, err
		}

		source, err := tbl.SourceSchema.DS.SourceTask(stmt)
		if err != nil {
			u.Errorf("could not create source task: %v", err)
			return nil, err
		}
		// Some data sources provide their own projections
		if projector, ok := source.(datasource.Projection); ok {
			m.Projection, err = projector.Projection()
			if err != nil {
				u.Errorf("could not build projection %v", err)
				return nil, err
			}
		} else {
			//panic("must implement projection")
			u.Warnf("could not create projection for: %T", source)
		}
		if scanner, ok := source.(datasource.Scanner); !ok {
			u.Warnf("could not create scanner? %#v", source)
			return nil, fmt.Errorf("Must Implement Scanner")
		} else {
			sourceTask := exec.NewSource(from, scanner)
			tasks.Add(sourceTask)
		}
	} else {

		// for now, only support 1 join
		if len(stmt.From) != 2 {
			// We should just be able to fold them all-together beyond 3
			return nil, fmt.Errorf("3 or more Table/Join not currently implemented")
		}

		stmt.From[0].Rewrite(true, stmt)
		stmt.From[1].Rewrite(false, stmt)
		// This is a HACK, need a better way obviously of redoing limit on re-written queries
		// preferably a Streaming solution using Sequence tasks
		stmt.From[0].Source.Limit = 100000000
		stmt.From[1].Source.Limit = 100000000

		in, err := exec.NewSourceJoin(m, stmt.From[0], stmt.From[1], m.svr.RtConf)
		if err != nil {
			return nil, err
		}
		tasks.Add(in)
	}

	// Add a Projection
	m.Projection = m.createProjection(stmt)

	return tasks, nil
}

func (m *Builder) createProjection(stmt *expr.SqlSelect) *expr.Projection {

	if m.Projection != nil {
		u.Debugf("allready has projection? %#v", m.Projection)
		return m.Projection
	}
	u.Debugf("createProjection %s", stmt.String())
	p := expr.NewProjection()
	for _, from := range stmt.From {
		u.Infof("info: %#v", from)
		tbl, err := m.schema.Table(strings.ToLower(from.Name))
		if err != nil {
			u.Errorf("could not get table: %v", err)
			return nil
		} else if tbl == nil {
			u.Errorf("no table? %v", from.Name)
		} else {
			u.Infof("getting cols? %v", len(from.Columns))
			if len(from.Columns) == 0 && len(stmt.From) == 1 {
				from.Columns = stmt.Columns
			}
			for _, col := range from.Columns {
				if schemaCol, ok := tbl.FieldMap[col.SourceField]; ok {
					u.Infof("adding projection col: %v %v", col.As, schemaCol.Type.String())
					p.AddColumnShort(col.As, schemaCol.Type)
				} else {
					u.Errorf("schema col not found:  vals=%#v", col)
				}
			}
		}
	}
	return p
}

func (m *Builder) VisitSelectDatabase(stmt *expr.SqlSelect) (interface{}, error) {
	u.Debugf("VisitSelectDatabase %+v", stmt)

	tasks := make(exec.Tasks, 0)
	val := "NULL"
	if m.schema != nil {
		val = m.schema.Name
	}
	static := inmemmap.NewStaticDataValue(val, "database")
	sourceTask := exec.NewSource(nil, static)
	tasks.Add(sourceTask)
	m.Projection = StaticProjection("database", value.StringType)
	return tasks, nil
}

func (m *Builder) VisitSubselect(stmt *expr.SqlSource) (interface{}, error) {
	u.Debugf("VisitSubselect %+v", stmt)
	u.LogTracef(u.WARN, "who?")
	return nil, expr.ErrNotImplemented
}

func (m *Builder) VisitJoin(stmt *expr.SqlSource) (interface{}, error) {
	u.Debugf("VisitJoin %+v", stmt)
	return nil, expr.ErrNotImplemented
}
