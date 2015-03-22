package backends

import (
	"fmt"
	"github.com/araddon/qlbridge/value"
	//"strings"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/expr"
	"github.com/dataux/dataux/pkg/models"
)

var (
	_ = u.EMPTY

	// Ensure that we implement the Exec Visitor interface
	_ exec.Visitor = (*Builder)(nil)
	//_ exec.JobRunner = (*Builder)(nil)
)

const (
	MaxAllowedPacket = 1024 * 1024
)

/*

Source ->  Where  -> GroupBy/Counts etc  -> Projection -> ResultWriter

- Since we don't really need the Where, GroupBy, etc

Source ->    Projection  -> ResultWriter


type JobRunner interface {
	Run() error
	Close() error
}

*/

// Create Job made up of sub-tasks in DAG that is the
//   plan for execution of this query/job
func BuildSqlJob(svr *models.ServerCtx, schemaDb, sqlText string) (*Builder, error) {

	stmt, err := expr.ParseSql(sqlText)
	if err != nil {
		u.Errorf("Could not parse: %v", err)
		return nil, err
	}

	builder := NewBuilder(svr, schemaDb)
	ex, err := stmt.Accept(builder)

	if err != nil {
		u.Errorf("Could not build %v", err)
		return nil, err
	}
	if ex == nil {
		// If No Error, and no Exec Tasks, then we already wrote results
		return nil, nil
	}
	tasks, ok := ex.(exec.Tasks)
	if !ok {
		return nil, fmt.Errorf("expected tasks but got: %T", ex)
	}
	builder.Job = &exec.SqlJob{tasks, stmt}
	return builder, nil
}

// This is a Sql Plan Builder that chooses backends
//   and routes/manages Requests
type Builder struct {
	schema     *models.Schema
	svr        *models.ServerCtx
	Projection *expr.Projection
	Job        *exec.SqlJob
	//where      expr.Node
	//children exec.Tasks
	//writer   models.ResultWriter
}

func NewBuilder(svr *models.ServerCtx, db string) *Builder {
	m := Builder{svr: svr}
	m.schema = svr.Schema(db)
	return &m
}

func (m *Builder) VisitSelect(stmt *expr.SqlSelect) (interface{}, error) {
	u.Debugf("VisitSelect %+v", stmt)

	if sysVar := stmt.SysVariable(); len(sysVar) > 0 {
		return m.VisitSysVariable(stmt)
	}

	tasks := make(exec.Tasks, 0)

	if len(stmt.From) > 1 {
		return nil, fmt.Errorf("join not implemented")
	}
	from := stmt.From[0]

	// source is of type qlbridge.datasource.DataSource
	source, err := m.schema.DataSource.SourceTask(stmt)
	if err != nil {
		return nil, err
	}
	// Some data sources provide their own projections
	if projector, ok := source.(models.SourceProjection); ok {
		m.Projection, err = projector.Projection()
		if err != nil {
			u.Errorf("could not build projection %v", err)
			return nil, err
		}
	} else {
		panic("must implement projection")
	}
	if scanner, ok := source.(datasource.Scanner); !ok {
		return nil, fmt.Errorf("Must Implement Scanner")
	} else {
		sourceTask := exec.NewSourceScanner(from.Name, scanner)
		tasks.Add(sourceTask)
	}
	return tasks, nil
}

func (m *Builder) VisitSysVariable(stmt *expr.SqlSelect) (interface{}, error) {
	u.Debugf("VisitSysVariable %+v", stmt)
	tasks := make(exec.Tasks, 0)
	switch sysVar := stmt.SysVariable(); sysVar {
	case "@@max_allowed_packet":
		// TODO:   build a simple Result Task
		static := datasource.NewStaticDataValue(MaxAllowedPacket, sysVar)
		sourceTask := exec.NewSourceScanner("system", static)
		m.Projection = StaticProjection(sysVar, value.IntType)
		tasks.Add(sourceTask)
		return tasks, nil
		// r, _ := proxy.BuildSimpleSelectResult(MaxAllowedPacket, []byte(sysVar), nil)
		// return nil, m.writer.WriteResult(r)
	default:
		u.Errorf("unknown var: %v", sysVar)
		return nil, fmt.Errorf("Unrecognized System Variable: %v", sysVar)
	}
	return nil, exec.ErrNotImplemented
}

func StaticProjection(name string, vt value.ValueType) *expr.Projection {
	p := expr.NewProjection()
	p.AddColumnShort(name, vt)
	return p
}
func (m *Builder) VisitInsert(stmt *expr.SqlInsert) (interface{}, error) {
	u.Debugf("VisitInsert %+v", stmt)
	return nil, exec.ErrNotImplemented
}

func (m *Builder) VisitUpsert(stmt *expr.SqlUpsert) (interface{}, error) {
	u.Debugf("VisitUpsert %+v", stmt)
	return nil, exec.ErrNotImplemented
}

func (m *Builder) VisitDelete(stmt *expr.SqlDelete) (interface{}, error) {
	u.Debugf("VisitDelete %+v", stmt)
	return nil, exec.ErrNotImplemented
}

func (m *Builder) VisitUpdate(stmt *expr.SqlUpdate) (interface{}, error) {
	u.Debugf("VisitUpdate %+v", stmt)
	return nil, exec.ErrNotImplemented
}

func (m *Builder) VisitShow(stmt *expr.SqlShow) (interface{}, error) {
	u.Debugf("VisitShow %+v", stmt)
	return nil, exec.ErrNotImplemented
}

func (m *Builder) VisitDescribe(stmt *expr.SqlDescribe) (interface{}, error) {
	u.Debugf("VisitDescribe %+v", stmt)
	return nil, exec.ErrNotImplemented
}

func (m *Builder) VisitPreparedStmt(stmt *expr.PreparedStatement) (interface{}, error) {
	u.Debugf("VisitPreparedStmt %+v", stmt)
	return nil, exec.ErrNotImplemented
}
