package mysqlfe

import (
	"fmt"
	"strings"
	"time"

	u "github.com/araddon/gou"
	"github.com/kr/pretty"

	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/lex"
	"github.com/araddon/qlbridge/plan"
	"github.com/araddon/qlbridge/rel"
	"github.com/araddon/qlbridge/schema"
	"github.com/araddon/qlbridge/value"

	"github.com/dataux/dataux/models"
	"github.com/dataux/dataux/vendored/mixer/mysql"
	mysqlproxy "github.com/dataux/dataux/vendored/mixer/proxy"
)

func init() {
	// Register our Mysql Frontend Listener
	models.ListenerRegister(mysqlproxy.ListenerType, &MySqlConnCreator{})
}

const (
	// Default Max Allowed packets for connections
	MaxAllowedPacket = 4194304
	//MaxAllowedPacketStr = "4194304"
)

var (
	_ = value.ErrValue
	_ = u.EMPTY
	_ = pretty.Diff

	// Ensure we meet our interfaces
	_ models.Listener         = (*MySqlConnCreator)(nil)
	_ models.StatementHandler = (*mySqlHandler)(nil)
)

// MySql connection handler, a single connection session
//  not threadsafe, not shared
type MySqlConnCreator struct {
	svr  *models.ServerCtx
	conf *models.ListenerConfig
	l    models.Listener
}

// Init is part of frontend interface to accept config and global server context at start
func (m *MySqlConnCreator) Init(conf *models.ListenerConfig, svr *models.ServerCtx) error {
	l, err := mysqlproxy.ListenerInit(conf, svr.Config, m)
	if err != nil {
		u.Errorf("could not init mysql listener: %v", err)
		return err
	}
	m.l = l
	m.svr = svr
	m.conf = conf
	return nil
}

// Open handler is a per-client/conn copy of handler
// - this occurs once when a new tcp-conn is established
// - it re-uses the HandlerShard with has schema, etc on it
func (m *MySqlConnCreator) Open(connI interface{}) models.StatementHandler {

	handler := mySqlHandler{svr: m.svr}

	if conn, ok := connI.(*mysqlproxy.Conn); ok {
		//u.Debugf("Cloning Mysql handler %v", conn)
		handler.conn = conn
		handler.connId = conn.ConnId()
		handler.sess = NewMySqlSessionVars("default", conn.User(), conn.ConnId())
		return &handler
	}
	panic(fmt.Sprintf("not proxy.Conn? %T", connI))
}
func (m *MySqlConnCreator) Run(stop chan bool) error {
	return m.l.Run(stop)
}

func (m *MySqlConnCreator) Close() error {
	// TODO Better close management
	return nil
}

func (m *MySqlConnCreator) String() string {
	return fmt.Sprintf("Mysql Frontend address:%v", m.conf.Addr)
}

// MySql per connection, ie session specific
type mySqlHandler struct {
	svr    *models.ServerCtx
	sess   expr.ContextReadWriter // session info
	conn   *mysqlproxy.Conn       // Connection to client, inbound mysql conn
	schema *schema.Schema
	connId uint32
}

func (m *mySqlHandler) Close() error {
	if m.conn != nil {
		err := m.conn.Close()
		m.conn = nil
		return err
	}
	return nil
}

// Handle Implement the Handle interface for frontends that processes requests
func (m *mySqlHandler) Handle(writer models.ResultWriter, req *models.Request) error {
	return m.chooseCommand(writer, req)
}

// Session level schema Use command of sql
func (m *mySqlHandler) SchemaUse(db string) *schema.Schema {
	schema, ok := m.svr.Schema(db)
	if schema == nil || !ok {
		u.Warnf("Could not find schema for db=%s", db)
		return nil
	}
	m.schema = schema
	m.sess = NewMySqlSessionVars(db, m.conn.User(), m.connId)
	return schema
}

func (m *mySqlHandler) chooseCommand(writer models.ResultWriter, req *models.Request) error {

	// First byte of mysql is a "command" type
	cmd := req.Raw[0]
	req.Raw = req.Raw[1:] // the rest is the statement which will get parsed

	//u.Debugf("chooseCommand: %v:%v", cmd, mysql.CommandString(cmd))
	switch cmd {
	case mysql.COM_FIELD_LIST:
		// mysql is going to deprecate it, so we don't support it
		msg := fmt.Sprintf("command %d:%s is deprecated", cmd, mysql.CommandString(cmd))
		return mysql.NewError(mysql.ER_WARN_DEPRECATED_SYNTAX, msg)
	case mysql.COM_QUERY, mysql.COM_STMT_PREPARE:
		return m.handleQuery(writer, string(req.Raw))
	case mysql.COM_PING:
		return m.writeOK(nil)
	case mysql.COM_QUIT:
		//u.Warnf("who is asking me to Quit? %s", string(cmd))
		m.Close()
		return nil
	case mysql.COM_INIT_DB:
		if s := m.SchemaUse(string(req.Raw)); s == nil {
			return fmt.Errorf("Schema not found %s", string(req.Raw))
		} else {
			return m.writeOK(nil)
		}
	// case mysql.COM_STMT_EXECUTE:
	// case mysql.COM_STMT_CLOSE:
	// case mysql.COM_STMT_SEND_LONG_DATA:
	// case mysql.COM_STMT_RESET:
	default:
		msg := fmt.Sprintf("command %d:%s not yet supported", cmd, mysql.CommandString(cmd))
		return mysql.NewError(mysql.ER_UNKNOWN_ERROR, msg)
	}

	return nil
}

func (m *mySqlHandler) handleQuery(writer models.ResultWriter, sql string) (err error) {

	u.Debugf("%d %p handleQuery: %v", m.connId, m, sql)
	if !m.svr.Config.SupressRecover {
		defer func() {
			if e := recover(); e != nil {
				u.Errorf("recover? %v", e)
				err = fmt.Errorf("handle query %s error %v", sql, e)
				return
			}
		}()
	}

	if m.schema == nil {
		s, err := m.svr.InfoSchema()
		if err != nil {
			u.Warnf("no infoschema? %v", err)
			return err
		}
		m.schema = s.InfoSchema
	}

	start := time.Now()
	ctx := plan.NewContext(sql)
	ctx.DisableRecover = m.svr.Config.SupressRecover
	ctx.Session = m.sess
	ctx.Schema = m.schema
	ctx.Funcs = fr
	if ctx.Schema == nil {
		u.Warnf("no schema found in handler, this should not happen ")
	}
	//u.Debugf("handler job svr: %p  svr.Grid: %p", m.svr, m.svr.PlanGrid.Grid)
	job, err := BuildMySqlJob(m.svr, ctx)

	if err != nil {
		//u.Debugf("error? nilstmt?%v  err=%v", ctx.Stmt == nil, err)
		if ctx.Stmt != nil {
			switch ctx.Stmt.Keyword() {
			case lex.TokenRollback, lex.TokenCommit:
				// we don't currently support transactions
				return m.conn.WriteOK(nil)
			case lex.TokenSet:
				// set autocommit
				// SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ
				return m.conn.WriteOK(nil)
			}
		} else {
			sql = strings.ToLower(sql)
			switch {
			case strings.HasPrefix(sql, "rollback") || strings.HasPrefix(sql, "commit"):

			case strings.HasPrefix(sql, "set "):

			}
		}

		u.Debugf("error on parse sql statement: %v", err)
		return err
	}
	if job == nil {
		// we are done, already wrote results
		return nil
	}

	//u.Infof("job.Ctx %p   Session %p", job.Ctx, job.Ctx.Session)
	//job.Ctx.Session = m.sess

	var resultWriter exec.Task
	switch stmt := job.Ctx.Stmt.(type) {
	case *rel.SqlSelect:
		resultWriter = NewMySqlResultWriter(writer, job.Ctx)
	case *rel.SqlShow, *rel.SqlDescribe:
		resultWriter = NewMySqlSchemaWriter(writer, job.Ctx)
	case *rel.SqlInsert, *rel.SqlUpsert, *rel.SqlUpdate, *rel.SqlDelete:
		resultWriter = NewMySqlExecResultWriter(writer, job.Ctx)
	case *rel.SqlCommand:
		if stmt.Keyword() == lex.TokenRollback {
			return m.conn.WriteOK(nil)
		}
		err = job.Run()
		job.Close()
		if err != nil {
			return err
		}
		return m.conn.WriteOK(nil)
	default:
		u.Warnf("sql not supported?  %v  %T", stmt, stmt)
		return fmt.Errorf("statement type %T not supported", stmt)
	}

	// job.Finalize() will:
	//  - insert any network/distributed tasks to other worker nodes
	//  - wait for those nodes to be ready to run
	//  - append the result writer after those tasks
	err = job.Finalize(resultWriter)
	if err != nil {
		u.Errorf("error on finalize %v", err)
		return err
	}
	//u.Infof("mysqlhandler %p task.Run() start", job.RootTask)
	err = job.Run()
	//u.Infof("mysqlhandler %p task.Run() complete", job.RootTask)
	if err != nil {
		u.Errorf("error on Query.Run(): %v", err)
	}
	//u.Infof("mysqlhandler %p task.Close() start for %T", job.RootTask, job.RootTask)
	closeErr := job.Close()
	if closeErr != nil {
		u.Errorf("could not close ? %v", closeErr)
	}
	end := time.Now().Sub(start)
	u.Infof("completed in %v   ns: %v", end, time.Now().UnixNano()-start.UnixNano())
	//u.Infof("mysqlhandler %p task.Close() complete  err=%v", job.RootTask, err)
	return err
}

func (m *mySqlHandler) writeOK(r *mysql.Result) error {
	return m.conn.WriteOK(r)
}

func makeBindVars(args []interface{}) map[string]interface{} {
	bindVars := make(map[string]interface{}, len(args))

	for i, v := range args {
		bindVars[fmt.Sprintf("v%d", i+1)] = v
	}

	return bindVars
}
