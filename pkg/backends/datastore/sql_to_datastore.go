package datastore

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/cloud/datastore"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/lex"
	"github.com/araddon/qlbridge/plan"
	"github.com/araddon/qlbridge/value"
	"github.com/araddon/qlbridge/vm"
)

var (
	DefaultLimit = 20

	_ = json.Marshal

	// planner
	_ plan.SourceSelectPlanner = (*SqlToDatstore)(nil)
)

// Sql To Google Datastore Maps a Sql request into an equivalent
//    google data store query
type SqlToDatstore struct {
	*exec.TaskBase
	resp           *ResultReader
	tbl            *datasource.Table
	plan           *plan.SourcePlan
	sel            *expr.SqlSelect
	stmt           expr.SqlStatement
	schema         *datasource.SourceSchema
	dsCtx          context.Context
	dsClient       *datastore.Client
	query          *datastore.Query
	hasMultiValue  bool // Multi-Value vs Single-Value aggs
	hasSingleValue bool // single value agg
}

func NewSqlToDatstore(table *datasource.Table, cl *datastore.Client, ctx context.Context) *SqlToDatstore {
	return &SqlToDatstore{
		tbl:      table,
		schema:   table.SourceSchema,
		dsCtx:    ctx,
		dsClient: cl,
		TaskBase: exec.NewTaskBase("SqlToDatstore"),
	}
}

func (m *SqlToDatstore) Host() string {
	//u.Warnf("TODO:  replace hardcoded es host")
	//return m.schema.ChooseBackend()
	return ""
}

func (m *SqlToDatstore) Query(req *expr.SqlSelect) (*ResultReader, error) {

	m.query = datastore.NewQuery(m.tbl.NameOriginal)
	//u.Debugf("%s   query:%p", m.tbl.NameOriginal, m.query)
	var err error
	m.sel = req
	limit := req.Limit
	if limit == 0 {
		limit = DefaultLimit
	}

	if req.Where != nil {
		err = m.WalkWhereNode(req.Where.Expr)
		if err != nil {
			u.Warnf("Could Not evaluate Where Node %s %v", req.Where.Expr.String(), err)
			return nil, err
		}
	}

	// Evaluate the Select columns
	//err = m.WalkSelectList()
	// if err != nil {
	// 	u.Warnf("Could Not evaluate Columns/Aggs %s %v", req.Columns.String(), err)
	// 	return nil, err
	// }

	// if len(req.GroupBy) > 0 {
	// 	err = m.WalkGroupBy()
	// 	if err != nil {
	// 		u.Warnf("Could Not evaluate GroupBys %s %v", req.GroupBy.String(), err)
	// 		return nil, err
	// 	}
	// }

	//u.Debugf("OrderBy? %v", len(m.sel.OrderBy))
	if len(m.sel.OrderBy) > 0 {
		for _, col := range m.sel.OrderBy {
			// We really need to look at any funcs?   walk this out
			switch col.Order {
			case "ASC":
				m.query = m.query.Order(fmt.Sprintf("%q", col.Key()))
			case "DESC":
				m.query = m.query.Order(fmt.Sprintf("-%q", col.Key()))
			default:
				m.query = m.query.Order(fmt.Sprintf("%q", col.Key()))
			}
		}
	}

	resultReader := NewResultReader(m)
	m.resp = resultReader
	//resultReader.Finalize()
	return resultReader, nil
}

func (m *SqlToDatstore) VisitSourceSelect(sp *plan.SourcePlan) (expr.Task, expr.VisitStatus, error) {
	//u.Debugf("VisitSourceSelect(): %T  %#v", visitor, visitor)
	// TODO:   this is really bad, this should not be a type switch
	//         something pretty wrong upstream, that the Plan doesn't do the walk visitor
	u.Debugf("VisitSourceSelect():  %T  %#v", sp, sp)
	m.plan = sp
	reader, err := m.Query(sp.SqlSource.Source)
	if err != nil {
		return nil, expr.VisitError, nil
	}
	return reader, expr.VisitContinue, nil
}

// interface for SourceMutation
//CreateMutator(stmt expr.SqlStatement) (Mutator, error)
func (m *SqlToDatstore) CreateMutator(stmt expr.SqlStatement) (datasource.Mutator, error) {
	m.stmt = stmt
	return m, nil
}

// interface for Upsert.Put()
func (m *SqlToDatstore) Put(ctx context.Context, key datasource.Key, val interface{}) (datasource.Key, error) {

	if key == nil {
		u.Debugf("didn't have key?  %v", val)
		//return nil, fmt.Errorf("Must have key for updates in DataStore")
	}

	if m.schema == nil {
		u.Warnf("must have schema")
		return nil, fmt.Errorf("Must have schema for updates in DataStore")
	}

	var dskey *datastore.Key
	props := make([]datastore.Property, 0)

	cols := m.tbl.Columns()
	var row []driver.Value
	u.Infof("PUT columns? %v", cols)
	curRow := make([]driver.Value, len(cols))

	if key != nil {
		dskey = datastore.NewKey(m.dsCtx, m.tbl.NameOriginal, fmt.Sprintf("%v", key.Key()), 0, nil)
	}

	switch sqlReq := m.stmt.(type) {
	case *expr.SqlInsert:
		cols = sqlReq.ColumnNames()
	case *expr.SqlUpdate:
		// need to fetch first?
		sel := sqlReq.SqlSelect()
		//u.Debugf("new Select:  %s", sel)
		res, err := m.Query(sel)
		if err != nil {
			u.Errorf("wat?  %v", err)
			return nil, err
		}

		if len(res.Vals) == 1 {
			curRow = res.Vals[0]
		} else {
			u.Warnf("should only have one to update in Put(): %v", len(res.Vals))
		}
	}

	switch valT := val.(type) {
	case []driver.Value:
		row = valT
		u.Infof("row len=%v   fieldlen=%v col len=%v", len(row), len(m.tbl.Fields), len(cols))
		for _, f := range m.tbl.Fields {
			for i, colName := range cols {
				if f.Name == colName {

					switch val := row[i].(type) {
					case string, []byte, int, int64, bool, time.Time:
						//u.Debugf("PUT field: i=%d col=%s row[i]=%v  T:%T", i, colName, row[i], row[i])
						props = append(props, datastore.Property{Name: f.Name, Value: val})
					case []value.Value:
						by, err := json.Marshal(val)
						if err != nil {
							u.Errorf("Error converting field %v  err=%v", val, err)
						}
						//u.Debugf("PUT field: i=%d col=%s row[i]=%v  T:%T", i, colName, string(by), by)
						props = append(props, datastore.Property{Name: f.Name, Value: by})
					default:
						u.Warnf("unsupported conversion: %T  %v", val, val)
						props = append(props, datastore.Property{Name: f.Name, Value: val})
					}

					break
				}
			}
		}
		// Create the key by position?  HACK
		dskey = datastore.NewKey(m.dsCtx, m.tbl.NameOriginal, fmt.Sprintf("%v", row[0]), 0, nil)

	case map[string]driver.Value:
		for i, f := range m.tbl.Fields {
			for colName, driverVal := range valT {
				if f.Name == colName {
					u.Debugf("PUT field: i=%d col=%s val=%v  T:%T", i, colName, driverVal, driverVal)
					switch val := driverVal.(type) {
					case string, []byte, int, int64, bool:

						curRow[i] = val
					case time.Time:
						curRow[i] = &val
					case []value.Value:
						by, err := json.Marshal(val)
						if err != nil {
							u.Errorf("Error converting field %v  err=%v", val, err)
						}
						curRow[i] = by
					default:
						u.Warnf("unsupported conversion: %T  %v", val, val)

					}
					break
				}
			}
			//u.Infof(" %v curRow? %d %#v", f.Name, len(curRow), curRow)
			props = append(props, datastore.Property{Name: f.Name, Value: curRow[i]})
		}

	default:
		u.Warnf("unsupported type: %T  %#v", val, val)
		return nil, fmt.Errorf("Was not []driver.Value?  %T", val)
	}

	//u.Debugf("has key? sourcekey: %v  dskey:%#v", key, dskey)
	//u.Debugf("dskey:  %s   table=%s", dskey, m.tbl.NameOriginal)
	//u.Debugf("props:  %v", props)

	pl := datastore.PropertyList(props)
	dskey, err := m.dsClient.Put(m.dsCtx, dskey, &pl)
	if err != nil {
		u.Errorf("could not save? %v", err)
		return nil, err
	}
	newKey := datasource.NewKeyCol("id", dskey.String())
	return newKey, nil
}

func (m *SqlToDatstore) PutMulti(ctx context.Context, keys []datasource.Key, src interface{}) ([]datasource.Key, error) {
	return nil, fmt.Errorf("not implemented")
}

func (m *SqlToDatstore) Delete(key driver.Value) (int, error) {
	dskey := datastore.NewKey(m.dsCtx, m.tbl.NameOriginal, fmt.Sprintf("%v", key), 0, nil)
	u.Infof("dskey:  %s   table=%s", dskey, m.tbl.NameOriginal)
	err := m.dsClient.Delete(m.dsCtx, dskey)
	if err != nil {
		u.Errorf("could not delete? %v", err)
		return 0, err
	}
	return 1, nil
}
func (m *SqlToDatstore) DeleteExpression(where expr.Node) (int, error) {
	delKey := datasource.KeyFromWhere(where)
	if delKey != nil {
		return m.Delete(delKey.Key())
	}
	return 0, fmt.Errorf("Could not delete with that where expression: %s", where)
}

// WalkWhereNode() an expression, and its AND logic to create an appropriately
//  request for google datastore queries
//
//  Limititations of Google Datastore
//  - https://cloud.google.com/datastore/docs/concepts/queries#Datastore_Restrictions_on_queries
//  - no OR filters
//  -
func (m *SqlToDatstore) WalkWhereNode(cur expr.Node) error {
	//u.Debugf("WalkWhereNode: %s", cur)
	switch curNode := cur.(type) {
	case *expr.NumberNode, *expr.StringNode:
		nodeVal, ok := vm.Eval(nil, cur)
		if !ok {
			u.Warnf("not ok %v", cur)
			return fmt.Errorf("could not evaluate: %v", cur.String())
		}
		u.Infof("nodeval? %v", nodeVal)
		return nil
		// What do we do here?
	case *expr.BinaryNode:
		return m.walkFilterBinary(curNode)
	case *expr.TriNode: // Between
		return fmt.Errorf("Between and other Tri-Node ops not implemented")
	case *expr.UnaryNode:
		return fmt.Errorf("Not implemented urnary function: %v", curNode.String())
	case *expr.FuncNode:
		return fmt.Errorf("Not implemented function: %v", curNode.String())
	case *expr.IdentityNode:
		u.Warnf("what uses identity node in Where?  %v", curNode.String())
		return fmt.Errorf("Not implemented identity node: %v", curNode.String())
	case *expr.MultiArgNode:
		return fmt.Errorf("Not implemented multi arg: %v", curNode.String())
	default:
		u.Warnf("eh?  %T %#v", cur, cur)
		return fmt.Errorf("Not implemented node: %v", curNode.String())
	}
	return nil
}

// Walk Binary Node:   convert to mostly Filters if possible
//
//	x = y             =>   x = y
//  x != y            =>   there are special limits in Google Datastore, must only have one unequal filter
//  x like "list%"    =    prefix filter in
//
// TODO:  Poly Fill features
//  x like "%list%"
//  - ancestor filters?
func (m *SqlToDatstore) walkFilterBinary(node *expr.BinaryNode) error {

	// How do we detect if this is a prefix query?  Probably would
	// have a column-level flag on schema?
	lhval, lhok := vm.Eval(nil, node.Args[0])
	rhval, rhok := vm.Eval(nil, node.Args[1])
	if !lhok || !rhok {
		u.Warnf("not ok: %v  l:%v  r:%v", node, lhval, rhval)
		return fmt.Errorf("could not evaluate: %v", node.String())
	}
	u.Debugf("walkBinary: %v  l:%v  r:%v  %T  %T", node, lhval, rhval, lhval, rhval)
	switch node.Operator.T {
	case lex.TokenLogicAnd:
		// AND is assumed by datastore
		for _, arg := range node.Args {
			err := m.WalkWhereNode(arg)
			if err != nil {
				u.Errorf("could not evaluate where nodes? %v %s", err, arg)
				return fmt.Errorf("could not evaluate: %s", arg.String())
			}
		}
	case lex.TokenLogicOr:
		return fmt.Errorf("DataStore does not implement OR: %v", node.String())
	case lex.TokenEqual, lex.TokenEqualEqual:
		//u.Debugf("query: %p", m.query)
		m.query = m.query.Filter(fmt.Sprintf("%q =", lhval.ToString()), rhval.Value())
	case lex.TokenNE:
		// WARNING:  datastore only allows 1, warn?
		m.query = m.query.Filter(fmt.Sprintf("%q !=", lhval.ToString()), rhval.Value())
	case lex.TokenLE:
		m.query = m.query.Filter(fmt.Sprintf("%q <=", lhval.ToString()), rhval.Value())
	case lex.TokenLT:
		m.query = m.query.Filter(fmt.Sprintf("%q <", lhval.ToString()), rhval.Value())
	case lex.TokenGE:
		m.query = m.query.Filter(fmt.Sprintf("%q >=", lhval.ToString()), rhval.Value())
	case lex.TokenGT:
		m.query = m.query.Filter(fmt.Sprintf("%q >", lhval.ToString()), rhval.Value())
	case lex.TokenLike:
		// Ancestors support some type of prefix query?
		// this only works on String columns?
		lhs := lhval.ToString()
		if strings.HasPrefix(lhs, "%") {
			// Need to polyFill?  Or Error
			return fmt.Errorf("Google Datastore does not support % prefix", lhs)
		}
		m.query = m.query.Filter(fmt.Sprintf("%q >=", lhs), rhval.Value())
	default:
		u.Warnf("not implemented: %v", node.Operator)
		return fmt.Errorf("not implemented %v", node.String())
	}
	return nil
}

/*
func (m *SqlToDatstore) walkFilterTri(node *expr.TriNode, q *bson.M) (value.Value, error) {

	arg1val, aok := vm.Eval(nil, node.Args[0])
	//u.Debugf("arg1? %v  ok?%v", arg1val, aok)
	arg2val, bok := vm.Eval(nil, node.Args[1])
	arg3val, cok := vm.Eval(nil, node.Args[2])
	u.Debugf("walkTri: %v  %v %v %v", node, arg1val, arg2val, arg3val)
	if !aok || !bok || !cok {
		return nil, fmt.Errorf("Could not evaluate args: %v", node.String())
	}
	u.Debugf("walkTri: %v  %v %v %v", node, arg1val, arg2val, arg3val)
	switch node.Operator.T {
	case lex.TokenBetween:
		*q = bson.M{arg1val.ToString(): bson.M{"$gte": arg2val.Value(), "$lte": arg3val.Value()}}
	default:
		u.Warnf("not implemented ")
	}
	if q != nil {
		return nil, nil
	}
	return nil, fmt.Errorf("not implemented")
}

// Mutli Arg expressions:
//
//		year IN (1990,1992)  =>
//
func (m *SqlToDatstore) walkMultiFilter(node *expr.MultiArgNode, q *bson.M) (value.Value, error) {

	// First argument must be field name in this context
	fldName := node.Args[0].String()
	u.Debugf("walkMulti: %v", node.String())
	switch node.Operator.T {
	case lex.TokenIN:
		//q = bson.M{"range": bson.M{arg1val.ToString(): bson.M{"gte": arg2val.ToString(), "lte": arg3val.ToString()}}}
		terms := make([]interface{}, len(node.Args)-1)
		*q = bson.M{fldName: bson.M{"$in": terms}}
		for i := 1; i < len(node.Args); i++ {
			// Do we eval here?
			v, ok := vm.Eval(nil, node.Args[i])
			if ok {
				u.Debugf("in? %T %v value=%v", v, v, v.Value())
				terms[i-1] = v.Value()
			} else {
				u.Warnf("could not evaluate arg: %v", node.Args[i])
			}
		}
	default:
		u.Warnf("not implemented %v", node.String())
		return nil, fmt.Errorf("Not implemented: %v", node.String())
	}
	if q != nil {
		u.Debug(string(u.JsonHelper(*q).PrettyJson()))
		return nil, nil
	}
	return nil, fmt.Errorf("Uknown Error")
}



// Take an expression func, ensure we don't do runtime-checking (as the function)
//   doesn't really exist, then map that function to an ES Filter
//
//    exists(fieldname)
//    regex(fieldname,value)
//
func (m *SqlToDatstore) walkFilterFunc(node *expr.FuncNode, q *bson.M) (value.Value, error) {
	switch funcName := strings.ToLower(node.Name); funcName {
	case "exists", "missing":
		op := true
		if funcName == "missing" {
			op = false
		}
		// { field: { $exists: <boolean> } }
		fieldName := ""
		if len(node.Args) != 1 {
			return nil, fmt.Errorf("Invalid func")
		}
		switch node.Args[0].NodeType() {
		case expr.IdentityNodeType:
			fieldName = node.Args[0].String()
		default:
			val, ok := eval(node.Args[0])
			if !ok {
				u.Errorf("Must be valid: %v", node.String())
			} else {
				fieldName = val.ToString()
			}
		}
		*q = bson.M{fieldName: bson.M{"$exists": op}}
	case "regex":
		// user_id regex("%bc%")
		// db.users.find( { user_id: /bc/ } )

		if len(node.Args) < 2 {
			return nil, fmt.Errorf(`Invalid func regex:  regex(fieldname,"/regvalue/i")`)
		}

		fieldVal, ok := eval(node.Args[0])
		if !ok {
			u.Errorf("Must be valid: %v", node.String())
			return value.ErrValue, fmt.Errorf(`Invalid func regex:  regex(fieldname,"/regvalue/i")`)
		}

		regexval, ok := eval(node.Args[0])
		if !ok {
			u.Errorf("Must be valid: %v", node.String())
			return value.ErrValue, fmt.Errorf(`Invalid func regex:  regex(fieldname,"/regvalue/i")`)
		}
		*q = bson.M{fieldVal.ToString(): bson.M{"$regex": regexval.ToString()}}
	default:
		u.Warnf("not implemented ")
	}
	u.Debugf("func:  %v", q)
	if q != nil {
		return nil, nil
	}
	return nil, fmt.Errorf("not implemented %v", node.String())
}

// Take an expression func, ensure we don't do runtime-checking (as the function)
//   doesn't really exist, then map that function to an ES aggregation
//
//    min, max, avg, sum, cardinality, terms
//
//   Single Value Aggregates:
//       min, max, avg, sum, cardinality, count
//
//  MultiValue aggregats:
//      terms, ??
//
func (m *SqlToDatstore) walkAggFunc(node *expr.FuncNode) (q bson.M, _ error) {
	switch funcName := strings.ToLower(node.Name); funcName {
	case "max", "min", "avg", "sum", "cardinality":
		m.hasSingleValue = true
		if len(node.Args) != 1 {
			return nil, fmt.Errorf("Invalid func")
		}
		val, ok := eval(node.Args[0])
		if !ok {
			u.Errorf("Must be valid: %v", node.String())
		}
		// "min_price" : { "min" : { "field" : "price" } }
		q = bson.M{funcName: bson.M{"field": val.ToString()}}
	case "terms":
		m.hasMultiValue = true
		// "products" : { "terms" : {"field" : "product", "size" : 5 }}

		if len(node.Args) == 0 || len(node.Args) > 2 {
			return nil, fmt.Errorf("Invalid terms function terms(field,10) OR terms(field)")
		}
		val, ok := eval(node.Args[0])
		if !ok {
			u.Errorf("Must be valid: %v", node.String())
		}
		if len(node.Args) >= 2 {
			size, ok := vm.Eval(nil, node.Args[1])
			if !ok {
				u.Errorf("Must be valid size: %v", node.Args[1].String())
			}
			// "products" : { "terms" : {"field" : "product", "size" : 5 }}
			q = bson.M{funcName: bson.M{"field": val.ToString(), "size": size.Value()}}
		} else {

			q = bson.M{funcName: bson.M{"field": val.ToString()}}
		}

	case "count":
		m.hasSingleValue = true
		u.Debugf("how do we want to use count(*)?  hit.hits?   or exists()?")
		val, ok := eval(node.Args[0])
		if !ok {
			u.Errorf("Must be valid: %v", node.String())
			return nil, fmt.Errorf("Invalid argument: %v", node.String())
		}
		if val.ToString() == "*" {
			return nil, nil
		} else {
			return bson.M{"exists": bson.M{"field": val.ToString()}}, nil
		}

	default:
		u.Warnf("not implemented ")
	}
	u.Debugf("func:  %v", q)
	if q != nil {
		return q, nil
	}
	return nil, fmt.Errorf("not implemented")
}

func eval(cur expr.Node) (value.Value, bool) {
	switch curNode := cur.(type) {
	case *expr.IdentityNode:
		return value.NewStringValue(curNode.Text), true
	case *expr.StringNode:
		return value.NewStringValue(curNode.Text), true
	default:
		u.Errorf("unrecognized T:%T  %v", cur, cur)
	}
	return value.NilValueVal, false
}


*/
/*
// Aggregations from the <select_list>
//
//    SELECT <select_list> FROM ... WHERE
//
func (m *SqlToDatstore) WalkSelectList() error {

	m.aggs = bson.M{}
	for i := len(m.sel.Columns) - 1; i >= 0; i-- {
		col := m.sel.Columns[i]
		//u.Debugf("i=%d of %d  %v %#v ", i, len(m.sel.Columns), col.Key(), col)
		if col.Expr != nil {
			switch curNode := col.Expr.(type) {
			// case *expr.NumberNode:
			// 	return nil, value.NewNumberValue(curNode.Float64), nil
			// case *expr.BinaryNode:
			// 	return m.walkBinary(curNode)
			// case *expr.TriNode: // Between
			// 	return m.walkTri(curNode)
			// case *expr.UnaryNode:
			// 	//return m.walkUnary(curNode)
			// 	u.Warnf("not implemented: %#v", curNode)
			case *expr.FuncNode:
				// All Func Nodes are Aggregates
				esm, err := m.WalkAggs(curNode)
				if err == nil && len(esm) > 0 {
					m.aggs[col.As] = esm
				} else if err != nil {
					u.Error(err)
					return err
				}
				//u.Debugf("esm: %v:%v", col.As, esm)
				//u.Debugf(curNode.String())
			// case *expr.MultiArgNode:
			// 	return m.walkMulti(curNode)
			// case *expr.IdentityNode:
			// 	return nil, value.NewStringValue(curNode.Text), nil
			// case *expr.StringNode:
			// 	return nil, value.NewStringValue(curNode.Text), nil
			default:
				//u.Debugf("likely a projection, not agg T:%T  %v", curNode, curNode)
				//panic("Unrecognized node type")
			}
		}

	}
	return nil
}


// Aggregations from the <select_list>
//
//    WHERE .. GROUP BY x,y,z
//
func (m *SqlToDatstore) WalkGroupBy() error {

	for i, col := range m.sel.GroupBy {
		if col.Expr != nil {
			u.Infof("Walk group by %s  %T", col.Expr.String(), col.Expr)
			switch col.Expr.(type) {
			case *expr.IdentityNode, *expr.FuncNode:
				esm := bson.M{}
				_, err := m.WalkNode(col.Expr, &esm)
				fld := strings.Replace(expr.FindIdentityField(col.Expr), ".", "", -1)
				u.Infof("gb: %s  %s", fld, u.JsonHelper(esm).PrettyJson())
				if err == nil {
					if len(m.innergb) > 0 {
						esm["aggs"] = bson.M{fmt.Sprintf("group_by_%d", i): m.innergb}
						// esm["aggs"] = bson.M{"group_by_" + fld: m.innergb}
					} else {
						esm = esm
					}
					m.innergb = esm
					u.Infof("esm: %v", esm)
				} else {
					u.Error(err)
					return err
				}

			}
		}
	}

	m.groupby = bson.M{"aggs": bson.M{"group_by": m.innergb}}
	return nil
}

// WalkAggs() aggregate expressions when used ast part of <select_list>
//  - For Aggregates (functions) it builds aggs
//  - For Projectsion (non-functions) it does nothing, that will be done later during projection
func (m *SqlToDatstore) WalkAggs(cur expr.Node) (q bson.M, _ error) {
	switch curNode := cur.(type) {
	// case *expr.NumberNode:
	// 	return nil, value.NewNumberValue(curNode.Float64), nil
	// case *expr.BinaryNode:
	// 	return m.walkBinary(curNode)
	// case *expr.TriNode: // Between
	// 	return m.walkTri(curNode)
	// case *expr.UnaryNode:
	// 	//return m.walkUnary(curNode)
	// 	u.Warnf("not implemented: %#v", curNode)
	case *expr.FuncNode:
		return m.walkAggFunc(curNode)
	// case *expr.MultiArgNode:
	// 	return m.walkMulti(curNode)
	// case *expr.IdentityNode:
	// 	return nil, value.NewStringValue(curNode.Text), nil
	// case *expr.StringNode:
	// 	return nil, value.NewStringValue(curNode.Text), nil
	default:
		u.Debugf("likely a projection, not agg T:%T  %v", cur, cur)
		//panic("Unrecognized node type")
	}
	// if cur.Negate {
	// 	q = bson.M{"not": q}
	// }
	return q, nil
}

*/
