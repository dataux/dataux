package proxy

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	u "github.com/araddon/gou"
	"github.com/kr/pretty"

	"github.com/araddon/qlbridge/expr/builtins"
	"github.com/araddon/qlbridge/rel"
	"github.com/araddon/qlbridge/schema"
	"github.com/araddon/qlbridge/value"

	"github.com/dataux/dataux/models"
	"github.com/dataux/dataux/vendored/mixer/client"
	"github.com/dataux/dataux/vendored/mixer/mysql"
	"github.com/dataux/dataux/vendored/mixer/router"
	"github.com/dataux/dataux/vendored/mixer/sqlparser"
)

func init() {
	builtins.LoadAllBuiltins()
}

/*
Handler Sharded is a mysql Router to take requests
and route, filter to backend connections

* Manage pool of db clients
* route
*/

var _ = value.ErrValue

// Schema is the schema for a named database, shared
// across multiple nodes
type SchemaSharded struct {
	*schema.SchemaSource
	mysqlnodes map[string]*Node
	rule       *router.Router
}

// Handle request splitting, a single connection session
// not threadsafe, not shared
type HandlerShardedShared struct {
	conf    *models.Config
	nodes   map[string]*Node
	schemas map[string]*SchemaSharded
	schema  *SchemaSharded
}

// Handle request splitting, a single connection session
// not threadsafe, not shared
type HandlerSharded struct {
	*HandlerShardedShared
	conn *Conn
}

// func NewHandlerSharded(conf *models.Config) (models.Listener, error) {
// 	sharedHandler := &HandlerShardedShared{
// 		conf: conf,
// 	}
// 	handler := &HandlerSharded{HandlerShardedShared: sharedHandler}
// 	err := handler.Init()
// 	return handler, err
// }

func (m *HandlerSharded) Init() error {

	if err := m.startBackendNodes(); err != nil {
		return err
	}

	return nil
}

func (m *HandlerShardedShared) Open(connI interface{}) models.StatementHandler {

	handler := HandlerSharded{HandlerShardedShared: m}
	if conn, ok := connI.(*Conn); ok {
		u.Debugf("Cloning shared handler %v", conn)
		handler.conn = conn
		return &handler
	}
	panic("not cloneable")
}

func (m *HandlerSharded) Close() error {
	return m.conn.Close()
}

func (m *HandlerSharded) Handle(writer models.ResultWriter, req *models.Request) error {
	return m.chooseCommand(writer, req)
}

func (m *HandlerSharded) SchemaUse(db string) *schema.Schema {
	schema, ok := m.schemas[db]
	if ok {
		m.schema = schema
		return schema.SchemaSource.Schema()
	}

	u.Errorf("Could not find schema for db=%s", db)
	return nil
}

func (m *HandlerSharded) chooseCommand(writer models.ResultWriter, req *models.Request) error {

	cmd := req.Raw[0]
	req.Raw = req.Raw[1:]

	u.Debugf("chooseCommand: %v:%v", cmd, mysql.CommandString(cmd))
	switch cmd {
	case mysql.COM_QUERY:
		// do we want unsafe pointer hack?
		return m.handleQuery(string(req.Raw))
	case mysql.COM_PING:
		return m.WriteOK(nil)
	case mysql.COM_STMT_PREPARE:
		return m.handleStmtPrepare(string(req.Raw))
	case mysql.COM_STMT_EXECUTE:
		return m.handleStmtExecute(req.Raw)
	case mysql.COM_QUIT:
		m.Close()
		return nil
	case mysql.COM_INIT_DB:
		if s := m.SchemaUse(string(req.Raw)); s == nil {
			msg := fmt.Sprintf("Database not found '%s'", string(req.Raw))
			return mysql.NewError(mysql.ER_UNKNOWN_ERROR, msg)
		} else {
			return m.WriteOK(nil)
		}
	case mysql.COM_FIELD_LIST:
		return m.handleFieldList(req.Raw)
	case mysql.COM_STMT_CLOSE:
		return m.handleStmtClose(req.Raw)
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

func (m *HandlerSharded) createSqlVm(sql string) (sqlVm rel.SqlStatement, err error) {
	defer func() {
		if e := recover(); e != nil {
			err = fmt.Errorf("could not parse query %s error %v", sql, e)
			u.Error(err)
		}
	}()
	sqlVm, err = rel.ParseSqlVm(sql)
	if err != nil {
		u.Errorf("could not parse sql vm %v", err)
	} else {
		u.Debugf("got sql vm: %T", sqlVm)
	}
	return
}

func (m *HandlerSharded) handleQuery(sql string) (err error) {
	u.Debugf("in handleQuery: %v", sql)
	if !m.conf.SupressRecover {
		//u.Debugf("running recovery? ")
		defer func() {
			if e := recover(); e != nil {
				err = fmt.Errorf("handle query %s error %v", sql, e)
				return
			}
		}()
	} else {
		//u.Debugf("Suppressing recovery? ")
	}

	sql = strings.TrimRight(sql, ";")

	// Just temp, ensure it parses
	sqlVm, err := m.createSqlVm(sql)
	if sqlVm != nil {
		switch v := sqlVm.(type) {
		case *rel.SqlDescribe:
			u.Warnf("describe not supported?  %v  %T", v, sqlVm)
			//return fmt.Errorf("Describe not supported yet")
			return m.handleDescribe(sql, v)
		case *rel.SqlShow:
			u.Warnf("show not supported?  %v  %T", v, sqlVm)
			//return m.handleShow(sql, v)
			//return fmt.Errorf("Show not supported yet")
		default:
			u.Warnf("sql not supported?  %v  %T", v, sqlVm)
			//return fmt.Errorf("statement not support now: %v", sqlVm.Statement.Keyword())
		}
	}

	var stmt sqlparser.Statement
	stmt, err = sqlparser.Parse(sql)
	if err != nil {
		u.Error(err)
		return fmt.Errorf(`parse sql "%s" error`, sql)
	}

	u.Debugf("handleQuery: %T ", stmt)
	switch v := stmt.(type) {
	case *sqlparser.Select:
		return m.handleSelect(v, sql, nil)
	case *sqlparser.Insert:
		return m.handleExec(stmt, sql, nil)
	case *sqlparser.Update:
		return m.handleExec(stmt, sql, nil)
	case *sqlparser.Delete:
		return m.handleExec(stmt, sql, nil)
	case *sqlparser.Replace:
		return m.handleExec(stmt, sql, nil)
	// case *sqlparser.Set:
	// 	return m.handleSet(v)
	// case *sqlparser.Begin:
	// 	return m.handleBegin()
	// case *sqlparser.Commit:
	// 	return m.handleCommit()
	// case *sqlparser.Rollback:
	// 	return m.handleRollback()
	case *sqlparser.SimpleSelect:
		return m.handleSimpleSelect(sql, v)
	case *sqlparser.Show:
		return m.handleShow(sql, v)
	// case *sqlparser.Admin:
	// 	return m.handleAdmin(v)
	default:
		u.Warnf("sql not supported?  %v  %T", v, stmt)
		return fmt.Errorf("statement %T not support now", stmt)
	}

	return nil
}

func (m *HandlerSharded) handleSelect(stmt *sqlparser.Select, sql string, args []interface{}) error {

	u.Debugf("handleSelect: %v", sql)
	bindVars := makeBindVars(args)

	sqlConns, err := m.getShardConns(true, stmt, bindVars)
	if err != nil {
		u.Error(err)
		return err
	} else if sqlConns == nil {
		u.Errorf("no sqlConns?  ")
		r := m.conn.NewEmptyResultsetOLD(stmt)
		return m.conn.WriteResultset(m.conn.Status, r)
	}

	var rs []*mysql.Result

	rs, err = m.executeInShard(sqlConns, sql, args)
	//u.Infof("handleSelect:  rs(%v)", len(rs))
	m.closeShardConns(sqlConns, false)

	if err == nil {
		//u.Infof("handleSelect:  rs(%v)", len(rs))
		err = m.conn.mergeSelectResult(rs, stmt)
	}

	return err
}

func (m *HandlerSharded) handleExec(stmt sqlparser.Statement, sql string, args []interface{}) error {

	bindVars := makeBindVars(args)

	conns, err := m.getShardConns(false, stmt, bindVars)
	if err != nil {
		return err
	} else if conns == nil {
		return m.conn.WriteOK(nil)
	}

	var rs []*mysql.Result

	if len(conns) == 1 {
		rs, err = m.executeInShard(conns, sql, args)
	} else {
		//for multi nodes, 2PC simple, begin, exec, commit
		//if commit error, data maybe corrupt
		for {
			if err = m.beginShardConns(conns); err != nil {
				break
			}

			if rs, err = m.executeInShard(conns, sql, args); err != nil {
				break
			}

			err = m.commitShardConns(conns)
			break
		}
	}

	m.closeShardConns(conns, err != nil)

	if err == nil {
		u.Debugf("handleExec calling mergeExecResult: %v", len(rs))
		err = m.conn.mergeExecResult(rs)
	}

	return err
}

func (m *HandlerSharded) handleSimpleSelect(sql string, stmt *sqlparser.SimpleSelect) error {

	if len(stmt.SelectExprs) != 1 {
		return fmt.Errorf("support select one information function, %s", sql)
	}

	expr, ok := stmt.SelectExprs[0].(*sqlparser.NonStarExpr)
	if !ok {
		return fmt.Errorf("support select information function, %s", sql)
	}

	var f *sqlparser.FuncExpr
	f, ok = expr.Expr.(*sqlparser.FuncExpr)
	if !ok {
		return fmt.Errorf("support select information function, %s", sql)
	}

	var r *mysql.Resultset
	var err error

	u.Debugf("perform handleSimpleSelect: %v", string(f.Name))
	switch strings.ToLower(string(f.Name)) {
	case "last_insert_id":
		r, err = BuildSimpleSelectResult(m.conn.lastInsertId, f.Name, expr.As)
	case "row_count":
		r, err = BuildSimpleSelectResult(m.conn.affectedRows, f.Name, expr.As)
	case "version":
		r, err = BuildSimpleSelectResult(mysql.ServerVersion, f.Name, expr.As)
	case "connection_id":
		r, err = BuildSimpleSelectResult(m.conn.connectionId, f.Name, expr.As)
	case "database":
		if m.schema != nil {
			r, err = BuildSimpleSelectResult(m.schema.Name, f.Name, expr.As)
		} else {
			r, err = BuildSimpleSelectResult("NULL", f.Name, expr.As)
		}
	default:
		u.Warnf("not supported: %v", string(f.Name))
		return fmt.Errorf("function %s not support", f.Name)
	}

	if err != nil {
		return err
	}

	return m.conn.WriteResultset(m.conn.Status, r)
}

func (m *HandlerSharded) handleFieldList(data []byte) error {

	index := bytes.IndexByte(data, 0x00)
	table := string(data[0:index])
	wildcard := string(data[index+1:])

	if m.schema == nil {
		return mysql.NewDefaultError(mysql.ER_NO_DB_ERROR)
	}

	nodeName := m.schema.rule.GetRule(table).Nodes[0]

	n := m.getNode(nodeName)

	co, err := n.getMasterConn()
	if err != nil {
		return err
	}
	defer co.Close()

	if err = co.UseDB(m.schema.Name); err != nil {
		return err
	}

	if fs, err := co.FieldList(table, wildcard); err != nil {
		return err
	} else {
		return m.conn.WriteFieldList(m.conn.Status, fs)
	}
}

func (m *HandlerSharded) handleShow(sql string, stmt *sqlparser.Show) error {
	var err error
	var r *mysql.Resultset
	switch strings.ToLower(stmt.Section) {
	case "databases":
		r, err = m.handleShowDatabases()
	case "tables":
		r, err = m.handleShowTables(sql, stmt)
	case "proxy":
		r, err = m.handleShowProxy(sql, stmt)
	default:
		err = fmt.Errorf("unsupport show %s now", sql)
	}

	if err != nil {
		return err
	}

	return m.conn.WriteResultset(m.conn.Status, r)
}

func (m *HandlerSharded) handleShowDatabases() (*mysql.Resultset, error) {
	dbs := make([]interface{}, 0, len(m.schemas))
	for key := range m.schemas {
		dbs = append(dbs, key)
	}

	return BuildSimpleShowResultset(dbs, "Database")
}

func (m *HandlerSharded) handleShowTables(sql string, stmt *sqlparser.Show) (*mysql.Resultset, error) {
	s := m.schema
	if stmt.From != nil {
		db := nstring(stmt.From)
		s = m.getSchema(db)
	}

	if s == nil {
		return nil, mysql.NewDefaultError(mysql.ER_NO_DB_ERROR)
	}

	var tables []string
	tmap := map[string]struct{}{}
	for _, n := range s.mysqlnodes {
		co, err := n.getMasterConn()
		if err != nil {
			return nil, err
		}

		if err := co.UseDB(s.Name); err != nil {
			co.Close()
			return nil, err
		}

		if r, err := co.Execute(sql); err != nil {
			co.Close()
			return nil, err
		} else {
			co.Close()
			for i := 0; i < r.RowNumber(); i++ {
				n, _ := r.GetString(i, 0)
				if _, ok := tmap[n]; !ok {
					tables = append(tables, n)
				}
			}
		}
	}

	sort.Strings(tables)

	values := make([]interface{}, len(tables))
	for i := range tables {
		values[i] = tables[i]
	}

	return BuildSimpleShowResultset(values, fmt.Sprintf("Tables_in_%s", s.Name))
}

func (m *HandlerSharded) handleDescribe(sql string, req *rel.SqlDescribe) error {
	s := m.schema
	if s == nil {
		return mysql.NewDefaultError(mysql.ER_NO_DB_ERROR)
	}

	for _, n := range s.mysqlnodes {
		co, err := n.getMasterConn()
		if err != nil {
			return err
		}

		if err := co.UseDB(s.Name); err != nil {
			co.Close()
			return err
		}

		if r, err := co.Execute(sql); err != nil {
			co.Close()
			return err
		} else {
			co.Close()
			rs := r.Resultset
			for _, f := range rs.Fields {

				u.Debugf("Field:  %s\n%# v", f.String(), pretty.Formatter(f))
			}
			for k, v := range rs.FieldNames {
				u.Debugf("FieldNames:  %s %v", k, v)
			}
			for _, f := range rs.Values {
				u.Debugf("Values:  %# v", pretty.Formatter(f))
			}
			for _, f := range rs.RowDatas {
				u.Debugf("RowDatas:  %s %# v", string(f), pretty.Formatter(f))
			}
			//u.Infof("%#v", r.Resultset)
			return m.conn.WriteResultset(m.conn.Status, rs)
		}

	}

	return fmt.Errorf("Could not process desribe")
}

func (m *HandlerSharded) handleShowProxy(sql string, stmt *sqlparser.Show) (*mysql.Resultset, error) {
	var err error
	var r *mysql.Resultset
	switch strings.ToLower(stmt.Key) {
	case "config":
		r, err = m.conn.HandleShowProxyConfig()
	case "status":
		r, err = m.handleShowProxyStatus(sql, stmt)
	default:
		err = fmt.Errorf("Unsupport show proxy [%v] yet, just support [config|status] now.", stmt.Key)
		u.Warn(err)
		return nil, err
	}
	return r, err
}

func (m *HandlerSharded) handleShowProxyStatus(sql string, stmt *sqlparser.Show) (*mysql.Resultset, error) {
	// TODO: handle like_or_where expr
	return nil, nil
}

func (m *HandlerSharded) handleStmtPrepare(sql string) error {
	if m.schema == nil {
		return mysql.NewDefaultError(mysql.ER_NO_DB_ERROR)
	}

	s := new(Stmt)

	sql = strings.TrimRight(sql, ";")

	var err error
	s.s, err = sqlparser.Parse(sql)
	if err != nil {
		return fmt.Errorf(`parse sql "%s" error`, sql)
	}

	s.sql = sql

	var tableName string
	switch s := s.s.(type) {
	case *sqlparser.Select:
		tableName = nstring(s.From)
	case *sqlparser.Insert:
		tableName = nstring(s.Table)
	case *sqlparser.Update:
		tableName = nstring(s.Table)
	case *sqlparser.Delete:
		tableName = nstring(s.Table)
	case *sqlparser.Replace:
		tableName = nstring(s.Table)
	default:
		return fmt.Errorf(`unsupport prepare sql "%s"`, sql)
	}

	r := m.schema.rule.GetRule(tableName)

	n := m.getNode(r.Nodes[0])

	if co, err := n.getMasterConn(); err != nil {
		return fmt.Errorf("prepare error %s", err)
	} else {
		defer co.Close()

		if err = co.UseDB(m.schema.Name); err != nil {
			return fmt.Errorf("parepre error %s", err)
		}

		if t, err := co.Prepare(sql); err != nil {
			return fmt.Errorf("parepre error %s", err)
		} else {

			s.params = t.ParamNum()
			s.columns = t.ColumnNum()
		}
	}

	s.id = atomic.AddUint32(&m.conn.stmtId, 1)

	if err = m.conn.writePrepare(s); err != nil {
		return err
	}

	s.ResetParams()

	m.conn.stmts[s.id] = s

	return nil
}

func (m *HandlerSharded) handleStmtExecute(data []byte) error {
	if len(data) < 9 {
		return mysql.ErrMalformPacket
	}

	pos := 0
	id := binary.LittleEndian.Uint32(data[0:4])
	pos += 4

	s, ok := m.conn.stmts[id]
	if !ok {
		return mysql.NewDefaultError(mysql.ER_UNKNOWN_STMT_HANDLER,
			strconv.FormatUint(uint64(id), 10), "stmt_execute")
	}

	flag := data[pos]
	pos++
	//now we only support CURSOR_TYPE_NO_CURSOR flag
	if flag != 0 {
		return mysql.NewError(mysql.ER_UNKNOWN_ERROR, fmt.Sprintf("unsupported flag %d", flag))
	}

	//skip iteration-count, always 1
	pos += 4

	var nullBitmaps []byte
	var paramTypes []byte
	var paramValues []byte

	paramNum := s.params

	if paramNum > 0 {
		nullBitmapLen := (s.params + 7) >> 3
		if len(data) < (pos + nullBitmapLen + 1) {
			return mysql.ErrMalformPacket
		}
		nullBitmaps = data[pos : pos+nullBitmapLen]
		pos += nullBitmapLen

		//new param bound flag
		if data[pos] == 1 {
			pos++
			if len(data) < (pos + (paramNum << 1)) {
				return mysql.ErrMalformPacket
			}

			paramTypes = data[pos : pos+(paramNum<<1)]
			pos += (paramNum << 1)

			paramValues = data[pos:]
		}

		if err := m.bindStmtArgs(s, nullBitmaps, paramTypes, paramValues); err != nil {
			return err
		}
	}

	var err error

	switch stmt := s.s.(type) {
	case *sqlparser.Select:
		err = m.handleSelect(stmt, s.sql, s.args)
	case *sqlparser.Insert:
		err = m.handleExec(s.s, s.sql, s.args)
	case *sqlparser.Update:
		err = m.handleExec(s.s, s.sql, s.args)
	case *sqlparser.Delete:
		err = m.handleExec(s.s, s.sql, s.args)
	case *sqlparser.Replace:
		err = m.handleExec(s.s, s.sql, s.args)
	default:
		err = fmt.Errorf("command %T not supported now", stmt)
	}

	s.ResetParams()

	return err
}

func (m *HandlerSharded) bindStmtArgs(s *Stmt, nullBitmap, paramTypes, paramValues []byte) error {
	args := s.args

	pos := 0

	var v []byte
	var n int = 0
	var isNull bool
	var err error

	for i := 0; i < s.params; i++ {
		if nullBitmap[i>>3]&(1<<(uint(i)%8)) > 0 {
			args[i] = nil
			continue
		}

		tp := paramTypes[i<<1]
		isUnsigned := (paramTypes[(i<<1)+1] & 0x80) > 0

		switch tp {
		case mysql.MYSQL_TYPE_NULL:
			args[i] = nil
			continue

		case mysql.MYSQL_TYPE_TINY:
			if len(paramValues) < (pos + 1) {
				return mysql.ErrMalformPacket
			}

			if isUnsigned {
				args[i] = uint8(paramValues[pos])
			} else {
				args[i] = int8(paramValues[pos])
			}

			pos++
			continue

		case mysql.MYSQL_TYPE_SHORT, mysql.MYSQL_TYPE_YEAR:
			if len(paramValues) < (pos + 2) {
				return mysql.ErrMalformPacket
			}

			if isUnsigned {
				args[i] = uint16(binary.LittleEndian.Uint16(paramValues[pos : pos+2]))
			} else {
				args[i] = int16((binary.LittleEndian.Uint16(paramValues[pos : pos+2])))
			}
			pos += 2
			continue

		case mysql.MYSQL_TYPE_INT24, mysql.MYSQL_TYPE_LONG:
			if len(paramValues) < (pos + 4) {
				return mysql.ErrMalformPacket
			}

			if isUnsigned {
				args[i] = uint32(binary.LittleEndian.Uint32(paramValues[pos : pos+4]))
			} else {
				args[i] = int32(binary.LittleEndian.Uint32(paramValues[pos : pos+4]))
			}
			pos += 4
			continue

		case mysql.MYSQL_TYPE_LONGLONG:
			if len(paramValues) < (pos + 8) {
				return mysql.ErrMalformPacket
			}

			if isUnsigned {
				args[i] = binary.LittleEndian.Uint64(paramValues[pos : pos+8])
			} else {
				args[i] = int64(binary.LittleEndian.Uint64(paramValues[pos : pos+8]))
			}
			pos += 8
			continue

		case mysql.MYSQL_TYPE_FLOAT:
			if len(paramValues) < (pos + 4) {
				return mysql.ErrMalformPacket
			}

			args[i] = float32(math.Float32frombits(binary.LittleEndian.Uint32(paramValues[pos : pos+4])))
			pos += 4
			continue

		case mysql.MYSQL_TYPE_DOUBLE:
			if len(paramValues) < (pos + 8) {
				return mysql.ErrMalformPacket
			}

			args[i] = math.Float64frombits(binary.LittleEndian.Uint64(paramValues[pos : pos+8]))
			pos += 8
			continue

		case mysql.MYSQL_TYPE_DECIMAL, mysql.MYSQL_TYPE_NEWDECIMAL, mysql.MYSQL_TYPE_VARCHAR,
			mysql.MYSQL_TYPE_BIT, mysql.MYSQL_TYPE_ENUM, mysql.MYSQL_TYPE_SET, mysql.MYSQL_TYPE_TINY_BLOB,
			mysql.MYSQL_TYPE_MEDIUM_BLOB, mysql.MYSQL_TYPE_LONG_BLOB, mysql.MYSQL_TYPE_BLOB,
			mysql.MYSQL_TYPE_VAR_STRING, mysql.MYSQL_TYPE_STRING, mysql.MYSQL_TYPE_GEOMETRY,
			mysql.MYSQL_TYPE_DATE, mysql.MYSQL_TYPE_NEWDATE,
			mysql.MYSQL_TYPE_TIMESTAMP, mysql.MYSQL_TYPE_DATETIME, mysql.MYSQL_TYPE_TIME:
			if len(paramValues) < (pos + 1) {
				return mysql.ErrMalformPacket
			}

			v, isNull, n, err = mysql.LengthEnodedString(paramValues[pos:])
			pos += n
			if err != nil {
				return err
			}

			if !isNull {
				args[i] = v
				continue
			} else {
				args[i] = nil
				continue
			}
		default:
			return fmt.Errorf("Stmt Unknown FieldType %d", tp)
		}
	}
	return nil
}

func (m *HandlerSharded) handleStmtSendLongData(data []byte) error {
	if len(data) < 6 {
		return mysql.ErrMalformPacket
	}

	id := binary.LittleEndian.Uint32(data[0:4])

	s, ok := m.conn.stmts[id]
	if !ok {
		return mysql.NewDefaultError(mysql.ER_UNKNOWN_STMT_HANDLER,
			strconv.FormatUint(uint64(id), 10), "stmt_send_longdata")
	}

	paramId := binary.LittleEndian.Uint16(data[4:6])
	if paramId >= uint16(s.params) {
		return mysql.NewDefaultError(mysql.ER_WRONG_ARGUMENTS, "stmt_send_longdata")
	}

	if s.args[paramId] == nil {
		s.args[paramId] = data[6:]
	} else {
		if b, ok := s.args[paramId].([]byte); ok {
			b = append(b, data[6:]...)
			s.args[paramId] = b
		} else {
			return fmt.Errorf("invalid param long data type %T", s.args[paramId])
		}
	}

	return nil
}

func (m *HandlerSharded) handleStmtReset(data []byte) error {
	if len(data) < 4 {
		return mysql.ErrMalformPacket
	}

	id := binary.LittleEndian.Uint32(data[0:4])

	s, ok := m.conn.stmts[id]
	if !ok {
		return mysql.NewDefaultError(mysql.ER_UNKNOWN_STMT_HANDLER,
			strconv.FormatUint(uint64(id), 10), "stmt_reset")
	}

	s.ResetParams()

	return m.conn.WriteOK(nil)
}

func (m *HandlerSharded) handleStmtClose(data []byte) error {
	if len(data) < 4 {
		return nil
	}

	id := binary.LittleEndian.Uint32(data[0:4])

	delete(m.conn.stmts, id)

	return nil
}

func (m *HandlerSharded) getShardList(stmt sqlparser.Statement, bindVars map[string]interface{}) ([]*Node, error) {

	if m.schema == nil {
		return nil, mysql.NewDefaultError(mysql.ER_NO_DB_ERROR)
	}

	u.Infof("getShardList: %v", m.schema.rule)
	ns, err := router.GetStmtShardList(stmt, m.schema.rule, bindVars)
	if err != nil {
		return nil, err
	}

	if len(ns) == 0 {
		return nil, nil
	}

	n := make([]*Node, 0, len(ns))
	for _, name := range ns {
		n = append(n, m.getNode(name))
	}
	return n, nil
}

func (m *HandlerSharded) getConn(n *Node, isSelect bool) (co *client.SqlConn, err error) {
	if !m.conn.needBeginTx() {
		if isSelect {
			co, err = n.getSelectConn()
		} else {
			co, err = n.getMasterConn()
		}
		if err != nil {
			return
		}
	} else {
		var ok bool
		m.conn.Lock()
		co, ok = m.conn.txConns[n]
		m.conn.Unlock()

		if !ok {
			if co, err = n.getMasterConn(); err != nil {
				return
			}

			if err = co.Begin(); err != nil {
				return
			}

			m.conn.Lock()
			m.conn.txConns[n] = co
			m.conn.Unlock()
		}
	}

	//todo, set conn charset, etm...
	if err = co.UseDB(m.schema.Name); err != nil {
		return
	}

	if err = co.SetCharset(m.conn.charset); err != nil {
		return
	}

	return
}

func (m *HandlerSharded) getShardConns(isSelect bool, stmt sqlparser.Statement, bindVars map[string]interface{}) ([]*client.SqlConn, error) {

	nodes, err := m.getShardList(stmt, bindVars)
	if err != nil {
		return nil, err
	} else if nodes == nil {
		return nil, nil
	}
	u.Infof("Get Shard List: %v  %#v", nodes, stmt)
	conns := make([]*client.SqlConn, 0, len(nodes))

	var co *client.SqlConn
	for _, n := range nodes {
		co, err = m.getConn(n, isSelect)
		if err != nil {
			break
		}

		conns = append(conns, co)
	}

	return conns, err
}

func (m *HandlerSharded) executeInShard(conns []*client.SqlConn, sql string, args []interface{}) ([]*mysql.Result, error) {
	var wg sync.WaitGroup
	wg.Add(len(conns))

	rs := make([]interface{}, len(conns))

	f := func(rs []interface{}, i int, co *client.SqlConn) {
		r, err := co.Execute(sql, args...)
		if err != nil {
			rs[i] = err
		} else {
			rs[i] = r
		}

		wg.Done()
	}

	for i, co := range conns {
		go f(rs, i, co)
	}

	wg.Wait()

	var err error
	r := make([]*mysql.Result, len(conns))
	for i, v := range rs {
		if e, ok := v.(error); ok {
			err = e
			break
		}
		r[i] = rs[i].(*mysql.Result)
		//u.Infof("executeInShard:  %#v  %#v", r[i], r[i].Resultset)
	}

	return r, err
}

func (m *HandlerSharded) closeShardConns(conns []*client.SqlConn, rollback bool) {
	if m.conn.isInTransaction() {
		return
	}

	for _, co := range conns {
		if rollback {
			co.Rollback()
		}

		co.Close()
	}
}

func (m *HandlerSharded) beginShardConns(conns []*client.SqlConn) error {
	if m.conn.isInTransaction() {
		return nil
	}

	for _, co := range conns {
		if err := co.Begin(); err != nil {
			return err
		}
	}

	return nil
}

func (m *HandlerSharded) commitShardConns(conns []*client.SqlConn) error {
	if m.conn.isInTransaction() {
		return nil
	}

	for _, co := range conns {
		if err := co.Commit(); err != nil {
			return err
		}
	}

	return nil
}

func (m *HandlerSharded) getNode(name string) *Node {
	return m.nodes[name]
}

func (m *HandlerSharded) startBackendNodes() error {

	m.nodes = make(map[string]*Node)

	panic("not implemented")

	/*
		for _, be := range m.conf.Sources {
			if be.SourceType == "" {
				for _, schemaConf := range m.conf.Schemas {
					for _, bename := range schemaConf.Nodes {
						if bename == be.Name {
							u.Infof("setting SourceType: %v", be.Name)
							be.SourceType = schemaConf.SourceType
						}
					}
				}
			}
			if be.SourceType == ListenerType {
				if _, ok := m.nodes[be.Name]; ok {
					return fmt.Errorf("duplicate node '%s'", be.Name)
				}

				n, err := m.startMysqlNode(be)
				if err != nil {
					return err
				}

				u.Infof("adding node: %s", be.String())
				m.nodes[be.Name] = n
			} else {
				u.Warnf("wrong backend type?: %#v", be)
			}
		}
	*/

	// if err := m.loadSchemasFromConfig(); err != nil {
	// 	return err
	// }

	return nil
}

/*
func (m *HandlerSharded) startMysqlNode(beConf *models.SourceConfig) (*Node, error) {

	n := new(Node)
	//n.listener = m
	n.cfg = beConf

	n.downAfterNoAlive = time.Duration(beConf.DownAfterNoAlive) * time.Second

	if len(beConf.Master) == 0 {
		return nil, fmt.Errorf("must setting master MySQL node.")
	}

	var err error
	if n.master, err = n.openDB(beConf.Master); err != nil {
		return nil, err
	}

	n.db = n.master

	if len(beConf.Slave) > 0 {
		if n.slave, err = n.openDB(beConf.Slave); err != nil {
			u.Errorf("open db error", err)
			n.slave = nil
		}
	}

	go n.run()

	return n, nil
}

func (m *HandlerSharded) loadSchemasFromConfig() error {

	m.schemas = make(map[string]*SchemaSharded)

	for _, schemaConf := range m.conf.Schemas {
		if schemaConf.SourceType == "mysql" {
			u.Infof("parse schemas: %v", schemaConf)
			if _, ok := m.schemas[schemaConf.DB]; ok {
				return fmt.Errorf("duplicate schema '%s'", schemaConf.DB)
			}
			if len(schemaConf.Nodes) == 0 {
				return fmt.Errorf("schema '%s' must have at least one node", schemaConf.DB)
			}

			//mysqlBackends := make(map[string]*models.Backend)
			mysqlNodes := make(map[string]*Node)
			for _, n := range schemaConf.Nodes {
				if m.getNode(n) == nil {
					return fmt.Errorf("schema '%s' node '%s' config does not exist", schemaConf.DB, n)
				}

				if _, ok := mysqlNodes[n]; ok {
					return fmt.Errorf("schema '%s' node '%s' is duplicate", schemaConf.DB, n)
				}

				mysqlNodes[n] = m.getNode(n)
			}

			rule, err := router.NewRouter(schemaConf)
			if err != nil {
				return err
			}

			schema := &models.Schema{
				Db: schemaConf.DB,
			}
			ss := &SchemaSharded{Schema: schema}
			ss.mysqlnodes = mysqlNodes
			ss.rule = rule
			m.schemas[schemaConf.DB] = ss
		} else {
			u.Infof("found schema not intended for this handler; %v", schemaConf.SourceType)
		}
	}

	return nil
}
*/

func (m *HandlerSharded) getSchema(db string) *SchemaSharded {
	u.Debugf("get schema for %s", db)
	return m.schemas[db]
}

func (m *HandlerSharded) WriteOK(r *mysql.Result) error {
	return m.conn.WriteOK(r)
}
