package mongo

import (
	"encoding/json"
	"fmt"
	"strings"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/lex"
	"github.com/araddon/qlbridge/value"
	"github.com/araddon/qlbridge/vm"
	"github.com/dataux/dataux/pkg/models"
)

var (
	DefaultLimit = 20

	_ = json.Marshal

	// planner
	_ datasource.SourcePlanner = (*SqlToMgo)(nil)
)

// Sql To Mongo Request
//   Map sql queries into Mongo bson Requests
type SqlToMgo struct {
	*exec.TaskBase
	resp           *ResultReader
	tbl            *models.Table
	sel            *expr.SqlSelect
	schema         *models.SourceSchema
	sess           *mgo.Session
	filter         bson.M
	aggs           bson.M
	groupby        bson.M
	innergb        bson.M // InnerMost Group By
	sort           []bson.M
	hasMultiValue  bool // Multi-Value vs Single-Value aggs
	hasSingleValue bool // single value agg
}

func NewSqlToMgo(table *models.Table, sess *mgo.Session) *SqlToMgo {
	return &SqlToMgo{
		tbl:      table,
		schema:   table.SourceSchema,
		sess:     sess,
		TaskBase: exec.NewTaskBase("SqlToMgo"),
	}
}

func (m *SqlToMgo) Host() string {
	//u.Warnf("TODO:  replace hardcoded es host")
	return m.schema.ChooseBackend()
}

func (m *SqlToMgo) Query(req *expr.SqlSelect) (*ResultReader, error) {

	var err error
	m.sel = req
	limit := req.Limit
	if limit == 0 {
		limit = DefaultLimit
	}

	if req.Where != nil {
		m.filter = bson.M{}
		_, err = m.WalkNode(req.Where.Expr, &m.filter)
		if err != nil {
			u.Warnf("Could Not evaluate Where Node %s %v", req.Where.Expr.StringAST(), err)
			return nil, err
		}
	}

	// Evaluate the Select columns
	err = m.WalkSelectList()
	if err != nil {
		u.Warnf("Could Not evaluate Columns/Aggs %s %v", req.Columns.String(), err)
		return nil, err
	}

	if len(req.GroupBy) > 0 {
		err = m.WalkGroupBy()
		if err != nil {
			u.Warnf("Could Not evaluate GroupBys %s %v", req.GroupBy.String(), err)
			return nil, err
		}
	}

	//u.Debugf("OrderBy? %v", len(m.sel.OrderBy))
	if len(m.sel.OrderBy) > 0 {
		m.sort = make([]bson.M, len(m.sel.OrderBy))
		for i, col := range m.sel.OrderBy {
			// We really need to look at any funcs?   walk this out
			switch col.Order {
			case "ASC":
				m.sort[i] = bson.M{col.As: 1}
			case "DESC":
				m.sort[i] = bson.M{col.As: -1}
			default:
				// default sorder order = ?
				m.sort[i] = bson.M{col.As: -1}
			}
		}
		var sort interface{}
		if len(m.sort) == 1 {
			sort = m.sort[0]
		} else {
			sort = m.sort
		}
		if len(m.filter) > 0 {
			m.filter = bson.M{"$query": m.filter, "$orderby": sort}
		} else {
			m.filter = bson.M{"$query": bson.M{}, "$orderby": sort}
		}
	}

	filterBy, _ := json.Marshal(m.filter)
	u.Infof("filter: %#v", m.filter)
	u.Debugf("db=%v  tbl=%v  \nfilter=%v \nsort=%v \nlimit=%v \nskip=%v", m.schema.Db, m.tbl.Name, string(filterBy), m.sort, req.Limit, req.Offset)
	query := m.sess.DB(m.schema.Db).C(m.tbl.Name).Find(m.filter).Limit(limit)

	resultReader := NewResultReader(m, query)
	m.resp = resultReader
	resultReader.Finalize()
	return resultReader, nil
}

func (m *SqlToMgo) Accept(visitor expr.SubVisitor) (datasource.Scanner, error) {
	//u.Debugf("Accept(): %T  %#v", visitor, visitor)
	// TODO:   this is really bad, this should not be a type switch
	//         something pretty wrong upstream, that the Plan doesn't do the walk visitor
	switch plan := visitor.(type) {
	case *exec.SourcePlan:
		u.Debugf("Accept():  %T  %#v", plan, plan)
		return m.Query(plan.SqlSource.Source)
	}
	return nil, expr.ErrNotImplemented
}

// Aggregations from the <select_list>
//
//    SELECT <select_list> FROM ... WHERE
//
func (m *SqlToMgo) WalkSelectList() error {

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
				//u.Debugf(curNode.StringAST())
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
func (m *SqlToMgo) WalkGroupBy() error {

	for i, col := range m.sel.GroupBy {
		if col.Expr != nil {
			u.Infof("Walk group by %s  %T", col.Expr.StringAST(), col.Expr)
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
func (m *SqlToMgo) WalkAggs(cur expr.Node) (q bson.M, _ error) {
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

// Walk() an expression, and its AND/OR/() logic to create an appropriately
//  nested bson document for mongo queries
//
//  TODO:  think we need to separate Value Nodes from those that return types?
func (m *SqlToMgo) WalkNode(cur expr.Node, q *bson.M) (value.Value, error) {
	//u.Debugf("WalkNode: %#v", cur)
	switch curNode := cur.(type) {
	case *expr.NumberNode, *expr.StringNode:
		nodeVal, ok := vm.Eval(nil, cur)
		if !ok {
			u.Warnf("not ok %v", cur)
			return nil, fmt.Errorf("could not evaluate: %v", cur.StringAST())
		}
		u.Infof("nodeval? %v", nodeVal)
		return nodeVal, nil
		// What do we do here?
	case *expr.BinaryNode:
		return m.walkFilterBinary(curNode, q)
	case *expr.TriNode: // Between
		return m.walkFilterTri(curNode, q)
	case *expr.UnaryNode:
		//return m.walkUnary(curNode)
		u.Warnf("not implemented: %#v", curNode)
		return nil, fmt.Errorf("Not implemented urnary function: %v", curNode.StringAST())
	case *expr.FuncNode:
		return m.walkFilterFunc(curNode, q)
	case *expr.IdentityNode:
		u.Warnf("wat????   %v", curNode.StringAST())
		*q = bson.M{"terms": bson.M{"field": curNode.String()}}
		return value.NewStringValue(curNode.String()), nil
	case *expr.MultiArgNode:
		return m.walkMultiFilter(curNode, q)
	default:
		u.Errorf("unrecognized T:%T  %v", cur, cur)
		panic("Unrecognized node type")
	}
	// if cur.Negate {
	// 	q = bson.M{"not": q}
	// }
	return nil, nil
}

func (m *SqlToMgo) walkFilterTri(node *expr.TriNode, q *bson.M) (value.Value, error) {

	//u.Infof("args: %#v", node.Args)
	arg1val, aok := vm.Eval(nil, node.Args[0])
	//u.Debugf("arg1? %v  ok?%v", arg1val, aok)
	arg2val, bok := vm.Eval(nil, node.Args[1])
	arg3val, cok := vm.Eval(nil, node.Args[2])
	//u.Debugf("walkTri: %v  %v %v %v", node, arg1val, arg2val, arg3val)
	if !aok || !bok || !cok {
		return nil, fmt.Errorf("Could not evaluate args: %v", node.StringAST())
	}
	//u.Debugf("walkTri: %v  %v %v %v", node, arg1val, arg2val, arg3val)
	switch node.Operator.T {
	case lex.TokenBetween:
		//u.Warnf("between? %T", arg2val.Value())
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
func (m *SqlToMgo) walkMultiFilter(node *expr.MultiArgNode, q *bson.M) (value.Value, error) {

	// First argument must be field name in this context
	fldName := node.Args[0].String()
	//u.Debugf("walkMulti: %v", node.StringAST())
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
		u.Warnf("not implemented %v", node.StringAST())
		return nil, fmt.Errorf("Not implemented: %v", node.StringAST())
	}
	if q != nil {
		u.Debug(string(u.JsonHelper(*q).PrettyJson()))
		return nil, nil
	}
	return nil, fmt.Errorf("Uknown Error")
}

// Simple Binary Node
//
//	x = y             =>   {field: {"$eq": value}}
//  x != y            =>   db.inventory.find( { qty: { $ne: 20 } } )
//
//  x like "list%"    =>   db.users.find( { user_id: /^list/ } )
//  x like "%list%"   =>   db.users.find( { user_id: /bc/ } )
//
func (m *SqlToMgo) walkFilterBinary(node *expr.BinaryNode, q *bson.M) (value.Value, error) {

	lhval, lhok := vm.Eval(nil, node.Args[0])
	rhval, rhok := vm.Eval(nil, node.Args[1])
	if !lhok || !rhok {
		u.Warnf("not ok: %v  l:%v  r:%v", node, lhval, rhval)
		return nil, fmt.Errorf("could not evaluate: %v", node.StringAST())
	}
	u.Debugf("walkBinary: %v  l:%v  r:%v  %T  %T", node, lhval, rhval, lhval, rhval)
	switch node.Operator.T {
	case lex.TokenLogicAnd:
		// this doesn't yet implement x AND y AND z
		lhq, rhq := bson.M{}, bson.M{}
		_, err := m.WalkNode(node.Args[0], &lhq)
		_, err2 := m.WalkNode(node.Args[1], &rhq)
		if err != nil || err2 != nil {
			u.Errorf("could not get children nodes? %v %v %v", err, err2, node)
			return nil, fmt.Errorf("could not evaluate: %v", node.StringAST())
		}
		*q = bson.M{"and": []bson.M{lhq, rhq}}
	case lex.TokenLogicOr:
		// this doesn't yet implement x AND y AND z
		lhq, rhq := bson.M{}, bson.M{}
		_, err := m.WalkNode(node.Args[0], &lhq)
		_, err2 := m.WalkNode(node.Args[1], &rhq)
		if err != nil || err2 != nil {
			u.Errorf("could not get children nodes? %v %v %v", err, err2, node)
			return nil, fmt.Errorf("could not evaluate: %v", node.StringAST())
		}
		*q = bson.M{"or": []bson.M{lhq, rhq}}
	case lex.TokenEqual, lex.TokenEqualEqual:
		// The $eq expression is equivalent to { field: <value> }.
		if lhval != nil && rhval != nil {
			*q = bson.M{lhval.ToString(): rhval.Value()}
			//u.Infof("m=%#v type=%v", q, rhval.Type())
			return nil, nil
		}
		if lhval != nil || rhval != nil {
			u.Infof("has stuff?  %v", node.StringAST())
		}
	case lex.TokenNE:
		// db.inventory.find( { qty: { $ne: 20 } } )
		*q = bson.M{lhval.ToString(): bson.M{"$ne": rhval.Value()}}
	case lex.TokenLE:
		// db.inventory.find( { qty: { $lte: 20 } } )
		*q = bson.M{lhval.ToString(): bson.M{"$lte": rhval.Value()}}
	case lex.TokenLT:
		// db.inventory.find( { qty: { $lt: 20 } } )
		*q = bson.M{lhval.ToString(): bson.M{"$lt": rhval.Value()}}
	case lex.TokenGE:
		// db.inventory.find( { qty: { $gte: 20 } } )
		*q = bson.M{lhval.ToString(): bson.M{"$gte": rhval.Value()}}
	case lex.TokenGT:
		// db.inventory.find( { qty: { $gt: 20 } } )
		*q = bson.M{lhval.ToString(): bson.M{"$gt": rhval.Value()}}
	case lex.TokenLike:
		// { $text: { $search: <string>, $language: <string> } }
		// { <field>: { $regex: /pattern/, $options: '<options>' } }

		rhs := rhval.ToString()
		startsPct := strings.Index(rhs, "%") == 0
		endsPct := strings.LastIndex(rhs, "%") == len(rhs)-1
		rhs = strings.Replace(rhs, "%", "", -1)
		u.Infof("LIKE:  %v  startsPCt?%v  endsPct?%v", rhs, startsPct, endsPct)
		// TODO:   this isn't working.   Not sure why
		if startsPct && endsPct {
			// user_id like "%bc%"
			// db.users.find( { user_id: /bc/ } )
			*q = bson.M{lhval.ToString(): bson.RegEx{fmt.Sprintf(`%s`, rhs), "i"}}
		} else if endsPct {
			//  WHERE user_id like "bc%"
			// db.users.find( { user_id: /^bc/ } )
			*q = bson.M{lhval.ToString(): bson.RegEx{fmt.Sprintf(`^%s`, rhs), "i"}}
		} else {
			*q = bson.M{lhval.ToString(): bson.RegEx{fmt.Sprintf(`%s`, rhs), "i"}}
		}

	default:
		u.Warnf("not implemented: %v", node.Operator)
	}
	if q != nil {
		return nil, nil
	}
	return nil, fmt.Errorf("not implemented %v", node.StringAST())
}

// Take an expression func, ensure we don't do runtime-checking (as the function)
//   doesn't really exist, then map that function to a mongo operation
//
//    exists(fieldname)
//    regex(fieldname,value)
//
func (m *SqlToMgo) walkFilterFunc(node *expr.FuncNode, q *bson.M) (value.Value, error) {
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
				u.Errorf("Must be valid: %v", node.StringAST())
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
			u.Errorf("Must be valid: %v", node.StringAST())
			return value.ErrValue, fmt.Errorf(`Invalid func regex:  regex(fieldname,"/regvalue/i")`)
		}

		regexval, ok := eval(node.Args[0])
		if !ok {
			u.Errorf("Must be valid: %v", node.StringAST())
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
	return nil, fmt.Errorf("not implemented %v", node.StringAST())
}

// Take an expression func, ensure we don't do runtime-checking (as the function)
//   doesn't really exist, then map that function to an Mongo Map function
//
//    min, max, avg, sum, cardinality, terms
//
//   Single Value Aggregates:
//       min, max, avg, sum, cardinality, count
//
//  MultiValue aggregats:
//      terms, ??
//
func (m *SqlToMgo) walkAggFunc(node *expr.FuncNode) (q bson.M, _ error) {
	switch funcName := strings.ToLower(node.Name); funcName {
	case "max", "min", "avg", "sum", "cardinality":
		m.hasSingleValue = true
		if len(node.Args) != 1 {
			return nil, fmt.Errorf("Invalid func")
		}
		val, ok := eval(node.Args[0])
		if !ok {
			u.Errorf("Must be valid: %v", node.StringAST())
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
			u.Errorf("Must be valid: %v", node.StringAST())
		}
		if len(node.Args) >= 2 {
			size, ok := vm.Eval(nil, node.Args[1])
			if !ok {
				u.Errorf("Must be valid size: %v", node.Args[1].StringAST())
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
			u.Errorf("Must be valid: %v", node.StringAST())
			return nil, fmt.Errorf("Invalid argument: %v", node.StringAST())
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
