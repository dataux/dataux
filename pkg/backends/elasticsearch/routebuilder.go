package elasticsearch

import (
	"fmt"

	u "github.com/araddon/gou"
	//"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/expr"
	"github.com/dataux/dataux/pkg/models"
)

var (
	_ = u.EMPTY

	// Ensure that we implement the Exec Visitor interface
	_ exec.Visitor = (*RoutePlanner)(nil)
)

//  TODO:   move to non-elasticsearch pkg

// Create Job made up of sub-tasks in DAG that is the
//   plan for execution of this query/job
func BuildSqlJob(conf *models.Config, sqlText string) (*exec.SqlJob, error) {

	stmt, err := expr.ParseSqlVm(sqlText)
	if err != nil {
		return nil, err
	}

	planner := NewRoutePlanner(conf)
	ex, err := stmt.Accept(planner)

	if err != nil {
		return nil, err
	}
	if ex == nil {
		return nil, fmt.Errorf("No job runner? %v", sqlText)
	}
	tasks, ok := ex.(exec.Tasks)
	if !ok {
		return nil, fmt.Errorf("expected tasks but got: %T", ex)
	}
	return &exec.SqlJob{tasks, stmt}, nil
}

// This is a Sql Plan Builder that chooses backends
//   and routes/manages REquests
type RoutePlanner struct {
	conf     *models.Config
	where    expr.Node
	distinct bool
	children exec.Tasks
}

func NewRoutePlanner(conf *models.Config) *RoutePlanner {
	m := RoutePlanner{}
	return &m
}

func (m *RoutePlanner) VisitSelect(stmt *expr.SqlSelect) (interface{}, error) {
	u.Debugf("VisitSelect %+v", stmt)

	if sysVar := stmt.SysVariable(); len(sysVar) > 0 {
		return m.VisitSysVariable(stmt)
	}
	//return m.handleSelect(sql, stmtNode, nil)

	tasks := make(exec.Tasks, 0)
	/*
		// Create our Datasource Reader
		var source datasource.DataSource
		if len(stmt.From) == 1 {
			from := stmt.From[0]
			if from.Name != "" && from.Source == nil {
				source = m.conf.DataSource(m.connInfo, from.Name)
				//u.Debugf("source: %T", source)
				in := NewSourceScanner(from.Name, source)
				tasks.Add(in)
			}

		} else {
			// if we have a join?
		}

		u.Debugf("has where? %v", stmt.Where != nil)
		if stmt.Where != nil {
			switch {
			case stmt.Where.Source != nil:
				u.Warnf("Found un-supported subquery: %#v", stmt.Where)
			case stmt.Where.Expr != nil:
				where := NewWhere(stmt.Where.Expr)
				tasks.Add(where)
			default:
				u.Warnf("Found un-supported where type: %#v", stmt.Where)
			}

		}

		// Add a Projection
		projection := NewProjection(stmt)
		tasks.Add(projection)
	*/

	return tasks, nil
}

func (m *RoutePlanner) VisitSysVariable(stmt *expr.SqlSelect) (interface{}, error) {
	u.Debugf("VisitSysVariable %+v", stmt)
	return nil, exec.ErrNotImplemented
}

func (m *RoutePlanner) VisitInsert(stmt *expr.SqlInsert) (interface{}, error) {
	u.Debugf("VisitInsert %+v", stmt)
	return nil, exec.ErrNotImplemented
}

func (m *RoutePlanner) VisitDelete(stmt *expr.SqlDelete) (interface{}, error) {
	u.Debugf("VisitDelete %+v", stmt)
	return nil, exec.ErrNotImplemented
}

func (m *RoutePlanner) VisitUpdate(stmt *expr.SqlUpdate) (interface{}, error) {
	u.Debugf("VisitUpdate %+v", stmt)
	return nil, exec.ErrNotImplemented
}

func (m *RoutePlanner) VisitShow(stmt *expr.SqlShow) (interface{}, error) {
	u.Debugf("VisitShow %+v", stmt)
	return nil, exec.ErrNotImplemented
}

func (m *RoutePlanner) VisitDescribe(stmt *expr.SqlDescribe) (interface{}, error) {
	u.Debugf("VisitDescribe %+v", stmt)
	return nil, exec.ErrNotImplemented
}
func (m *RoutePlanner) VisitPreparedStmt(stmt *expr.PreparedStatement) (interface{}, error) {
	u.Debugf("VisitPreparedStmt %+v", stmt)
	return nil, exec.ErrNotImplemented
}
