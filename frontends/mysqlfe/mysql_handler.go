package mysqlfe

import (
	"fmt"
	"strings"

	u "github.com/araddon/gou"
	"github.com/kr/pretty"

	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/plan"
	"github.com/araddon/qlbridge/rel"
	"github.com/araddon/qlbridge/schema"
	"github.com/araddon/qlbridge/value"

	"github.com/dataux/dataux/models"
	"github.com/dataux/dataux/vendored/mixer/mysql"
	mysqlproxy "github.com/dataux/dataux/vendored/mixer/proxy"
)

func init() {
	// Load our Frontend Listener's
	models.ListenerRegister(mysqlproxy.ListenerType, &MySqlConnCreator{})
}

const (
	// Default Max Allowed packets for connections
	MaxAllowedPacket = 4194304
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

func (m *MySqlConnCreator) Init(conf *models.ListenerConfig, svr *models.ServerCtx) error {
	//u.Debugf("mysql conn creator")
	l, err := mysqlproxy.ListenerInit(conf, svr.Config, m)
	if err != nil {
		u.Errorf("could not get mysql listener: %v", err)
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

	//u.Infof("Open: %#v", connI)
	handler := mySqlHandler{svr: m.svr}

	if conn, ok := connI.(*mysqlproxy.Conn); ok {
		//u.Debugf("Cloning Mysql handler %v", conn)
		handler.conn = conn
		handler.connId = conn.ConnId()
		handler.sess = NewMySqlSessionVars("default", conn.ConnId())
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

// MySql connection creator
// - per connection, ie session specific
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

// Implement the Handle interface for frontends
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
	m.sess = NewMySqlSessionVars(db, m.connId)
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

	u.Debugf("handleQuery: %v", sql)
	if !m.svr.Config.SupressRecover {
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
	ctx := plan.NewContext(sql)
	ctx.DisableRecover = m.svr.Config.SupressRecover
	ctx.Session = m.sess
	ctx.Schema = m.schema
	if ctx.Schema == nil {
		u.Warnf("no schema: ")
	} else {
		//u.Warnf("ctx has schema? %p", ctx.Schema)
	}
	job, err := BuildMySqlJob(m.svr, ctx)

	if err != nil {
		//u.Debugf("error? %v", err)
		sql = strings.ToLower(sql)
		switch {
		case strings.HasPrefix(sql, "set "):
			// set autocommit
			// SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ
			return m.conn.WriteOK(nil)
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
	//u.Infof("about to run")
	err = job.Run()
	//u.Infof("after run")
	if err != nil {
		u.Errorf("error on Query.Run(): %v", err)
	}
	job.Close()
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
