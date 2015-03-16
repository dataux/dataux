package frontends

import (
	"fmt"
	"strings"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/lex"
	"github.com/araddon/qlbridge/value"
	"github.com/dataux/dataux/pkg/backends"
	"github.com/dataux/dataux/pkg/models"
	"github.com/dataux/dataux/vendor/mixer/mysql"
	"github.com/dataux/dataux/vendor/mixer/proxy"
	"github.com/kr/pretty"
)

/*

Lets turn this into 4 pieces

- FrontEnd:
  - stmt := stmt.Parse(txt)
  - myresults := NewMysqlResultWriter(stmt)
  - err := NewHandler(myresults, stmt)

- SchemaManager:
  - schemagfind(stmt)

- Handler(resultWriter, stmt):
   job := newJobRunner(config, resultWriter)
   err := job.Accept(stmt)

- ResultWriter interface{}
  - implement MysqlResultWriter

*/

const (
	MaxAllowedPacket = 1024 * 1024
)

var (
	_ = value.ErrValue
	_ = u.EMPTY
	_ = pretty.Diff

	// Ensure we meet our interfaces
	_ models.HandlerSession = (*MySqlHandler)(nil)
	_ models.Handler        = (*MySqlHandler)(nil)
)

// Handle request splitting, a single connection session
// not threadsafe, not shared
type MySqlHandlerShared struct {
	svr    *models.ServerCtx
	conf   *models.Config
	nodes  map[string]*models.SourceConfig // List of servers
	schema *models.Schema
}

// Handle request splitting, a single connection session
// not threadsafe, not shared
type MySqlHandler struct {
	*MySqlHandlerShared
	conn *proxy.Conn
}

func NewMySqlHandler(svr *models.ServerCtx) (models.Handler, error) {
	sharedHandler := &MySqlHandlerShared{svr: svr, conf: svr.Config}
	err := sharedHandler.Init()
	connHandler := &MySqlHandler{MySqlHandlerShared: sharedHandler}
	return connHandler, err
}

func (m *MySqlHandlerShared) Init() error {

	u.Debugf("Init()")
	// if err := m.findEsNodes(); err != nil {
	// 	u.Errorf("could not init es: %v", err)
	// 	return err
	// }
	// if err := m.loadSchemasFromConfig(); err != nil {
	// 	return err
	// }
	return nil
}

func (m *MySqlHandler) Clone(connI interface{}) models.Handler {

	handler := MySqlHandler{MySqlHandlerShared: m.MySqlHandlerShared}
	if conn, ok := connI.(*proxy.Conn); ok {
		u.Debugf("Cloning shared handler %v", conn)
		handler.conn = conn
		return &handler
	}
	panic("not cloneable")
}

func (m *MySqlHandler) Close() error {
	return m.conn.Close()
}

func (m *MySqlHandler) Handle(writer models.ResultWriter, req *models.Request) error {
	return m.chooseCommand(writer, req)
}

func (m *MySqlHandler) SchemaUse(db string) *models.Schema {
	schema := m.svr.Schema(db)
	if schema != nil {
		m.schema = schema
		u.Debugf("Use Schema: %v", db)
	} else {
		u.Warnf("Could not find schema for db=%s", db)
	}
	return schema
}

func (m *MySqlHandler) chooseCommand(writer models.ResultWriter, req *models.Request) error {

	cmd := req.Raw[0]
	req.Raw = req.Raw[1:]

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
	job, err := backends.BuildSqlJob(m.svr, writer, m.schema.Db, sql)
	//stmt, err := expr.ParseSql(sql)
	if err != nil {
		u.Warnf("error? %v", err)
		sql = strings.ToLower(sql)
		switch {
		case strings.Contains(sql, "set autocommit"):
			return m.conn.WriteOK(nil)
		case strings.Contains(sql, "set session transaction isolation"):
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

	// resultWriter := NewResultRows(sqlSelect.Columns.FieldNames())
	// job.Tasks.Add(resultWriter)

	job.Setup()
	go func() {
		u.Debugf("Start Job.Run")
		err = job.Run()
		u.Debugf("After job.Run()")
		if err != nil {
			u.Errorf("error on Query.Run(): %v", err)
			//resultWriter.ErrChan() <- err
			//job.Close()
		}
		//job.Close()
		//u.Debugf("exiting Background Query")
	}()
	return nil

	switch stmtNode := job.Stmt.(type) {
	case *expr.SqlDescribe:
		switch {
		case stmtNode.Identity != "":
			return m.handleDescribeTable(sql, stmtNode)
		case stmtNode.Stmt != nil && stmtNode.Stmt.Keyword() == lex.TokenSelect:
			u.Infof("describe/explain Not Implemented: %#v", stmtNode)
		default:
			u.Warnf("unrecognized describe/explain: %#v", stmtNode)
		}
		return fmt.Errorf("describe/explain not yet supported: %#v", stmtNode)
	//case *expr.SqlSelect:
	case *expr.SqlShow:
		return m.handleShow(sql, stmtNode)
	// case *sqlparser.SimpleSelect:
	// 		return m.handleSimpleSelect(sql, stmtNode)
	// case *sqlparser.Insert:
	// 	return m.handleExec(stmt, sql, nil)
	// case *sqlparser.Update:
	// 	return m.handleExec(stmt, sql, nil)
	// case *sqlparser.Delete:
	// 	return m.handleExec(stmt, sql, nil)
	// case *sqlparser.Replace:
	// 	return m.handleExec(stmt, sql, nil)
	// case *sqlparser.Set:
	// 	return m.handleSet(stmtNode)
	// case *sqlparser.Begin:
	// 	return m.handleBegin()
	//case *sqlparser.Commit:
	// 	return m.handleCommit()
	// case *sqlparser.Rollback:
	// 	return m.handleRollback()
	// case *sqlparser.Admin:
	// 	return m.handleAdmin(stmtNode)
	default:
		u.Warnf("sql not supported?  %v  %T", stmtNode, stmtNode)
		return fmt.Errorf("statement type %T not supported", stmtNode)
	}

	return nil
}

func (m *MySqlHandler) handleDescribeTable(sql string, req *expr.SqlDescribe) error {

	/*
		s := m.schema
		if s == nil {
			return mysql.NewDefaultError(mysql.ER_NO_DB_ERROR)
		}
		tableName := strings.ToLower(req.Identity)

		tbl, err := m.loadTableSchema(tableName)
		if err != nil {
			return err
		}

		return m.conn.WriteResultset(m.conn.Status, tbl.DescribeResultset())
	*/
	return nil
}

func (m *MySqlHandler) handleShow(sql string, stmt *expr.SqlShow) error {
	var err error
	var r *mysql.Resultset
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

	if err != nil {
		return err
	}

	return m.conn.WriteResultset(m.conn.Status, r)
}

func (m *MySqlHandler) handleShowDatabases() (*mysql.Resultset, error) {
	/*
		dbs := make([]interface{}, 0, len(m.schemas))
		for key := range m.schemas {
			dbs = append(dbs, key)
		}

		return m.conn.BuildSimpleShowResultset(dbs, "Database")
	*/
	return nil, nil
}

func (m *MySqlHandler) handleShowTables(sql string, stmt *expr.SqlShow) (*mysql.Resultset, error) {
	/*
		s := m.schema
		if stmt.From != "" {
			s = m.getSchema(strings.ToLower(stmt.From))
		}

		if s == nil {
			u.Warnf("no schema? %v", stmt)
			return nil, mysql.NewDefaultError(mysql.ER_NO_DB_ERROR)
		}
		if len(s.TableNames) == 0 {
			u.Errorf("no tables? %#v", s)
			return nil, fmt.Errorf("No tables found?")
		}

		values := make([]interface{}, len(s.TableNames))
		for i, name := range s.TableNames {
			values[i] = name
		}
		u.Debugf("values: %v", values)
		return m.conn.BuildSimpleShowResultset(values, fmt.Sprintf("Tables_in_%s", s.Db))
	*/

	return nil, nil
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
