package backends

import (
	"fmt"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/exec"
	//"github.com/araddon/qlbridge/expr"
	"github.com/dataux/dataux/pkg/models"
)

var (
	_ = u.EMPTY

	// Ensure that we implement the Exec Visitor interface
	//_ expr.Visitor = (*Builder)(nil)

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
// This is a Sql Plan Builder that chooses backends
//   and routes/manages Requests
type Builder struct {
	//svr *models.ServerCtx
	*exec.SqlJob
	//*exec.JobBuilder
	//where      expr.Node
	//children exec.Tasks
	//writer   models.ResultWriter
}
*/
// Create Job made up of sub-tasks in DAG that is the
//   plan for execution of this query/job
func BuildSqlJob(svr *models.ServerCtx, schemaDb, sqlText string) (*exec.SqlJob, error) {

	return exec.BuildSqlProjectedJob(svr.RtConf, schemaDb, sqlText)
	/*
		stmt, err := expr.ParseSql(sqlText)
		if err != nil {
			u.Warnf("Could not parse: %v", err)
			return nil, err
		}

		sqlJob := &exec.SqlJob{
			Stmt: stmt,
			Conf: svr.RtConf,
		}
		execBuilder := exec.NewJobBuilder(svr.RtConf, schemaDb)

		builder := NewBuilder(svr, sqlJob, schemaDb)
		builder.JobBuilder = execBuilder

		//u.LogTracef(u.WARN, "BuildSqlJob: schema='%s'  %#v", schemaDb, builder.schema)
		u.Debugf("BuildSqlJob: schema='%s'  %#v", schemaDb, builder.Schema)
		task, err := stmt.Accept(builder)
		if err != nil {
			u.Warnf("Could not build %v", err)
			return nil, err
		}
		if task == nil {
			// If No Error, and no Exec Tasks, then we already wrote results
			return nil, nil
		}
		tr, ok := task.(exec.TaskRunner)
		if !ok {
			return nil, fmt.Errorf("Could not convert %T to TaskRunner", task)
		}
		builder.RootTask = tr

		return builder, nil
	*/

}

/*
// This is a Sql Plan Builder that chooses backends
//   and routes/manages Requests
type Builder struct {
	svr *models.ServerCtx
	*exec.SqlJob
	*exec.JobBuilder
	//where      expr.Node
	//children exec.Tasks
	//writer   models.ResultWriter
}

func NewBuilder(svr *models.ServerCtx, job *exec.SqlJob, db string) *Builder {
	m := Builder{svr: svr, SqlJob: job}
	u.Infof("builder db=%q   svr=%#v", db, svr)
	m.Schema = svr.Schema(db)
	if m.Schema == nil {
		u.Warnf("no schema? %v", db)
	}
	return &m
}

func (m *Builder) VisitSysVariable(stmt *expr.SqlSelect) (expr.Task, error) {
	//u.Debugf("VisitSysVariable %+v", stmt)

	switch sysVar := strings.ToLower(stmt.SysVariable()); sysVar {
	case "@@max_allowed_packet":
		return m.sysVarTasks(sysVar, MaxAllowedPacket)
	case "current_user()", "current_user":
		return m.sysVarTasks(sysVar, "user")
	case "connection_id()":
		return m.sysVarTasks(sysVar, 1)
	case "timediff(curtime(), utc_time())":
		return m.sysVarTasks("timediff", "00:00:00.000000")
		//
	default:
		u.Errorf("unknown var: %v", sysVar)
		return nil, fmt.Errorf("Unrecognized System Variable: %v", sysVar)
	}
}

// A very simple tasks/builder for system variables
//
func (m *Builder) sysVarTasks(name string, val interface{}) (expr.Task, error) {
	tasks := make(exec.Tasks, 0)
	static := membtree.NewStaticDataValue(name, val)
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
	return exec.NewSequential("sys-var", tasks), nil
}

// A very simple projection of name=value, for single row/column
//   select @@max_bytes
//
func StaticProjection(name string, vt value.ValueType) *expr.Projection {
	p := expr.NewProjection()
	p.AddColumnShort(name, vt)
	return p
}

func (m *Builder) VisitPreparedStmt(stmt *expr.PreparedStatement) (expr.Task, error) {
	u.Debugf("VisitPreparedStmt %+v", stmt)
	return nil, ErrNotImplemented
}

func (m *Builder) VisitCommand(stmt *expr.SqlCommand) (expr.Task, error) {
	u.Debugf("SqlCommand %+v", stmt)
	tasks := make(exec.Tasks, 0)
	return exec.NewSequential("sys-command", tasks), nil
}
*/
