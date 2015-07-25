package backends

import (
	"fmt"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
	"github.com/dataux/dataux/pkg/models"
)

var (
	_ = u.EMPTY

	// Ensure that we implement the Exec Visitor interface
	_ expr.Visitor = (*Builder)(nil)

	// Standard errors
	ErrNotSupported     = fmt.Errorf("DataUX: Not supported")
	ErrNotImplemented   = fmt.Errorf("DataUX: Not implemented")
	ErrUnknownCommand   = fmt.Errorf("DataUX: Unknown Command")
	ErrInternalError    = fmt.Errorf("DataUX: Internal Error")
	ErrNoSchemaSelected = fmt.Errorf("No Schema Selected")
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
		u.Warnf("Could not parse: %v", err)
		return nil, err
	}

	//u.Infof("BuildSqlJob: schema='%s'", schemaDb)
	builder := NewBuilder(svr, schemaDb)
	ex, err := stmt.Accept(builder)

	if err != nil {
		u.Warnf("Could not build %v", err)
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
	builder.Job = &exec.SqlJob{tasks, stmt, svr.RtConf}
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
	if m.schema == nil {
		u.Warnf("no schema? %v", db)
	}
	return &m
}

func (m *Builder) VisitSysVariable(stmt *expr.SqlSelect) (interface{}, error) {
	u.Debugf("VisitSysVariable %+v", stmt)

	switch sysVar := stmt.SysVariable(); sysVar {
	case "@@max_allowed_packet":
		return m.sysVarTasks(sysVar, MaxAllowedPacket)
	default:
		u.Errorf("unknown var: %v", sysVar)
		return nil, fmt.Errorf("Unrecognized System Variable: %v", sysVar)
	}
}

// A very simple tasks/builder for system variables
//
func (m *Builder) sysVarTasks(name string, val interface{}) (interface{}, error) {
	tasks := make(exec.Tasks, 0)
	static := datasource.NewStaticDataValue(val, name)
	sourceTask := exec.NewSource(nil, static)
	tasks.Add(sourceTask)
	switch val.(type) {
	case int, int64:
		m.Projection = StaticProjection(name, value.IntType)
	case string:
		m.Projection = StaticProjection(name, value.StringType)
	case float32, float64:
		m.Projection = StaticProjection(name, value.NumberType)
	case bool:
		m.Projection = StaticProjection(name, value.BoolType)
	default:
		u.Errorf("unknown var: %v", val)
		return nil, fmt.Errorf("Unrecognized Data Type: %v", val)
	}
	return tasks, nil
}

// A very simple projection of name=value, for single row/column
//   select @@max_bytes
//
func StaticProjection(name string, vt value.ValueType) *expr.Projection {
	p := expr.NewProjection()
	p.AddColumnShort(name, vt)
	return p
}
func (m *Builder) VisitInsert(stmt *expr.SqlInsert) (interface{}, error) {
	u.Debugf("VisitInsert %+v", stmt)
	return nil, ErrNotImplemented
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

func (m *Builder) VisitPreparedStmt(stmt *expr.PreparedStatement) (interface{}, error) {
	u.Debugf("VisitPreparedStmt %+v", stmt)
	return nil, ErrNotImplemented
}
