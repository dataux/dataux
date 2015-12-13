package frontends

import (
	"fmt"
	"strings"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
	"github.com/kr/pretty"

	"github.com/dataux/dataux/pkg/backends"
	"github.com/dataux/dataux/pkg/models"
	"github.com/dataux/dataux/vendor/mixer/mysql"
	"github.com/dataux/dataux/vendor/mixer/proxy"
)

const (
	// Default Max Allowed packets for connections
	MaxAllowedPacket = 4194304
)

var (
	_ = value.ErrValue
	_ = u.EMPTY
	_ = pretty.Diff

	// Ensure we meet our interfaces
	_ models.ConnectionHandle = (*MySqlHandler)(nil)
	_ models.Handler          = (*MySqlHandler)(nil)
)

// Handle request splitting, a single connection session
// not threadsafe, not shared
type MySqlHandlerShared struct {
	svr    *models.ServerCtx
	conf   *models.Config
	nodes  map[string]*datasource.SourceConfig // List of servers
	schema *datasource.Schema
}

// Handle request splitting, a single connection session
// not threadsafe, not shared
type MySqlHandler struct {
	*MySqlHandlerShared
	sess expr.ContextReader
	conn *proxy.Conn
}

func NewMySqlHandler(svr *models.ServerCtx) (models.ConnectionHandle, error) {
	sharedHandler := &MySqlHandlerShared{svr: svr, conf: svr.Config}
	err := sharedHandler.Init()
	connHandler := &MySqlHandler{MySqlHandlerShared: sharedHandler}
	return connHandler, err
}

func (m *MySqlHandlerShared) Init() error { return nil }

// Clone this handler as each handler is a per-client/conn copy of handler
// - this occurs once when a new tcp-conn is established
// - it re-uses the HandlerShard with has schema, etc on it
func (m *MySqlHandler) Open(connI interface{}) models.Handler {

	handler := MySqlHandler{MySqlHandlerShared: m.MySqlHandlerShared}
	handler.sess = NewMySqlSessionVars()

	if conn, ok := connI.(*proxy.Conn); ok {
		//u.Debugf("Cloning Mysql handler %v", conn)
		handler.conn = conn
		return &handler
	}
	panic("not cloneable")
}

func (m *MySqlHandler) Close() error {
	if m.conn != nil {
		err := m.conn.Close()
		m.conn = nil
		return err
	}
	return nil
}

// Implement the Handle interface for frontends
func (m *MySqlHandler) Handle(writer models.ResultWriter, req *models.Request) error {
	return m.chooseCommand(writer, req)
}

// Session level schema Use command of sql
func (m *MySqlHandler) SchemaUse(db string) *datasource.Schema {
	schema := m.svr.Schema(db)
	if schema == nil {
		u.Warnf("Could not find schema for db=%s", db)
		return nil
	}
	m.schema = schema
	return schema
}

func (m *MySqlHandler) chooseCommand(writer models.ResultWriter, req *models.Request) error {

	// First byte of mysql is a "command" type
	cmd := req.Raw[0]
	req.Raw = req.Raw[1:] // take the rest which will get parsed

	u.Debugf("chooseCommand: %v:%v", cmd, mysql.CommandString(cmd))
	switch cmd {
	case mysql.COM_QUERY, mysql.COM_STMT_PREPARE:
		return m.handleQuery(writer, string(req.Raw))
	case mysql.COM_PING:
		return m.writeOK(nil)
		//return m.handleStmtPrepare(string(req.Raw))
	// case mysql.COM_STMT_EXECUTE:
	// 	return m.handleStmtExecute(req.Raw)
	case mysql.COM_QUIT:
		m.Close()
		return nil
	case mysql.COM_INIT_DB:
		if s := m.SchemaUse(string(req.Raw)); s == nil {
			return fmt.Errorf("Schema not found %s", string(req.Raw))
		} else {
			return m.writeOK(nil)
		}
	// case mysql.COM_FIELD_LIST:
	// 	return m.handleFieldList(req.Raw)
	// case mysql.COM_STMT_CLOSE:
	// 	return m.handleStmtClose(req.Raw)
	// 	case mysql.COM_STMT_SEND_LONG_DATA:
	// 		return c.handleStmtSendLongData(req.Raw)
	// 	case mysql.COM_STMT_RESET:
	// 		return c.handleStmtReset(req.Raw)
	default:
		msg := fmt.Sprintf("command %d:%s not supported for now", cmd, mysql.CommandString(cmd))
		return mysql.NewError(mysql.ER_UNKNOWN_ERROR, msg)
	}

	return nil
}

func (m *MySqlHandler) handleQuery(writer models.ResultWriter, sql string) (err error) {
	u.Debugf("handleQuery: %v", sql)
	if !m.conf.SupressRecover {
		//u.Debugf("running recovery? ")
		defer func() {
			if e := recover(); e != nil {
				u.Errorf("recover? %v", e)
				err = fmt.Errorf("handle query %s error %v", sql, e)
				return
			}
		}()
	}

	if m.schema == nil {
		u.Warnf("missing schema?  ")
		return fmt.Errorf("no schema in use")
	}

	// Ensure it parses, right now we can't handle multiple statement (ie with semi-colons separating)
	// sql = strings.TrimRight(sql, ";")
	req := expr.NewContextConn(m.schema.Name, sql)
	req.Session = m.sess
	job, err := backends.BuildSqlJob(m.svr, req)
	if err != nil {
		u.Debugf("error? %v", err)
		sql = strings.ToLower(sql)
		switch {
		case strings.Contains(sql, "set autocommit"):
			return m.conn.WriteOK(nil)
		case strings.Contains(sql, "set session transaction isolation"):
			// SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ
			return m.conn.WriteOK(nil)
		case strings.HasPrefix(sql, "set "):
			return m.conn.WriteOK(nil)
		}
		u.Debugf("error on parse sql statement: %v", err)
		return err
	}
	if job == nil {
		// we are done, already wrote results
		return nil
	}
	u.Infof("job.Ctx %p   Session %p", job.Ctx, job.Ctx.Session)
	job.Ctx.Session = m.sess

	switch stmt := job.Stmt.(type) {
	case *expr.SqlSelect, *expr.SqlShow, *expr.SqlDescribe:
		//u.Debugf("adding mysql result writer: projection: %p", job.Projection)
		resultWriter := NewMySqlResultWriter(writer, job.Projection, m.schema)
		job.RootTask.Add(resultWriter)
	case *expr.SqlInsert, *expr.SqlUpsert, *expr.SqlUpdate, *expr.SqlDelete:
		//u.Debugf("adding mysql result writer: %#v", job.Projection)
		resultWriter := NewMySqlExecResultWriter(writer, m.schema)
		job.RootTask.Add(resultWriter)
	// case *sqlparser.Delete:
	// 	return m.handleExec(stmt, sql, nil)
	// case *sqlparser.Replace:
	// 	return m.handleExec(stmt, sql, nil)
	case *expr.SqlCommand:
		return m.conn.WriteOK(nil)
	// case *sqlparser.Begin:
	// 	return m.handleBegin()
	//case *sqlparser.Commit:
	// 	return m.handleCommit()
	// case *sqlparser.Rollback:
	// 	return m.handleRollback()
	// case *sqlparser.Admin:
	// 	return m.handleAdmin(stmt)
	default:
		u.Warnf("sql not supported?  %v  %T", stmt, stmt)
		return fmt.Errorf("statement type %T not supported", stmt)
	}

	job.Setup()
	err = job.Run()
	if err != nil {
		u.Errorf("error on Query.Run(): %v", err)
	}
	job.Close()
	return nil
}

func (m *MySqlHandler) writeOK(r *mysql.Result) error {
	return m.conn.WriteOK(r)
}

func makeBindVars(args []interface{}) map[string]interface{} {
	bindVars := make(map[string]interface{}, len(args))

	for i, v := range args {
		bindVars[fmt.Sprintf("v%d", i+1)] = v
	}

	return bindVars
}
