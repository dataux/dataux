package backends

import (
	"strings"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/expr"
)

var (
	_ = u.EMPTY
)

func (m *Builder) VisitShow(stmt *expr.SqlShow) (interface{}, error) {
	u.Debugf("VisitShow %+v", stmt)

	/*
		case *expr.SqlDescribe:
			switch {
			case stmt.Identity != "":
				return m.handleDescribeTable(sql, stmt)
			case stmt.Stmt != nil && stmt.Stmt.Keyword() == lex.TokenSelect:
				u.Infof("describe/explain Not Implemented: %#v", stmt)
			default:
				u.Warnf("unrecognized describe/explain: %#v", stmt)
			}
			return fmt.Errorf("describe/explain not yet supported: %#v", stmt)


		switch strings.ToLower(stmt.Identity) {
		case "databases":
			r, err = m.handleShowDatabases()
		case "tables":
			r, err = m.handleShowTables(sql, stmt)
		// case "proxy":
		// 	r, err = m.handleShowProxy(sql, stmt)
		default:
			err = fmt.Errorf("unsupport show %s now", sql)
		}

	*/

	source, proj := m.schema.ShowTables()
	m.Projection = proj

	tasks := make(exec.Tasks, 0)
	sourceTask := exec.NewSource("system", source)
	u.Infof("source:  %#v", source)
	tasks.Add(sourceTask)

	return tasks, nil
}

func (m *Builder) VisitDescribe(stmt *expr.SqlDescribe) (interface{}, error) {
	u.Debugf("VisitDescribe %+v", stmt)

	if m.schema == nil {
		return nil, ErrNoSchemaSelected
	}
	tbl, err := m.schema.Table(strings.ToLower(stmt.Identity))
	if err != nil {
		u.Errorf("could not get table: %v", err)
		return nil, err
	}
	source, proj := tbl.DescribeTable()
	m.Projection = proj

	tasks := make(exec.Tasks, 0)
	sourceTask := exec.NewSource("schema", source)
	u.Infof("source:  %#v", source)
	tasks.Add(sourceTask)

	return tasks, nil
}
