package mongo

import (
	"encoding/json"
	"fmt"
	"strings"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/lex"
	"github.com/araddon/qlbridge/plan"
	"github.com/araddon/qlbridge/rel"
	"github.com/araddon/qlbridge/schema"
	"github.com/araddon/qlbridge/value"
	"github.com/araddon/qlbridge/vm"
)

var (
	DefaultLimit = 20

	_ = json.Marshal

	// Implement Datasource interface that allows Mongo
	//  to fully implement a full select statement
	_ plan.SourcePlanner  = (*SqlToMgo)(nil)
	_ exec.ExecutorSource = (*SqlToMgo)(nil)
)

/*
	SourcePlanner interface {
		// given our request statement, turn that into a plan.Task.
		WalkSourceSelect(s *Source) (Task, error)
	}
*/

// Sql To Mongo Request
//   Map sql queries into Mongo bson Requests
type SqlToMgo struct {
	*exec.TaskBase
	resp           *ResultReader
	p              *plan.Source
	tbl            *schema.Table
	sel            *rel.SqlSelect
	schema         *schema.SourceSchema
	sess           *mgo.Session
	filter         bson.M
	aggs           bson.M
	groupby        bson.M
	innergb        bson.M // InnerMost Group By
	sort           []bson.M
	partition      *schema.Partition
	limit          int
	hasMultiValue  bool // Multi-Value vs Single-Value aggs
	hasSingleValue bool // single value agg
	needsPolyFill  bool // do we request that features be polyfilled?
}

func NewSqlToMgo(table *schema.Table, sess *mgo.Session) *SqlToMgo {
	sm := &SqlToMgo{
		tbl:    table,
		schema: table.SourceSchema,
		sess:   sess,
	}
	//u.Debugf("new SqlToMgo %p", sm)
	return sm
}

func (m *SqlToMgo) Columns() []string {
	return m.tbl.Columns()
}

// Called by Planner, pre-executor
func (m *SqlToMgo) WalkSourceSelect(planner plan.Planner, p *plan.Source) (plan.Task, error) {

	u.Debugf("WalkSourceSelect %p", m)
	p.Conn = m

	if len(p.Custom) == 0 {
		p.Custom = make(u.JsonHelper)
	}

	m.TaskBase = exec.NewTaskBase(p.Context())
	p.SourceExec = true
	m.p = p

	var err error
	m.p = p
	req := p.Stmt.Source
	//u.Infof("mongo.VisitSubSelect %v final:%v", req.String(), sp.Final)

	if p.Proj == nil {
		u.Warnf("%p no projection?  ", p)
		proj := plan.NewProjectionInProcess(p.Stmt.Source)
		p.Proj = proj.Proj
	} else {
		u.Infof("%p has projection!!! %s sqltomgo %p", p, p.Stmt, m)
		//u.LogTraceDf(u.WARN, 12, "hello")
	}

	m.sel = req

	m.limit = req.Limit
	if m.limit == 0 {
		m.limit = DefaultLimit
	}
	if !p.Final {
		m.limit = 1e10
	}

	if req.Where != nil {
		m.filter = bson.M{}
		_, err = m.WalkNode(req.Where.Expr, &m.filter)
		if err != nil {
			u.Warnf("Could Not evaluate Where Node %s %v", req.Where.Expr.String(), err)
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

	if m.needsPolyFill {
		p.Custom["poly_fill"] = true
		//u.Warnf("%p  need to signal poly-fill", m)
	} else {
		p.Complete = true
	}

	return nil, nil
}

func (m *SqlToMgo) WalkExecSource(p *plan.Source) (exec.Task, error) {

	if p.Stmt == nil {
		return nil, fmt.Errorf("Plan did not include Sql Statement?")
	}
	if p.Stmt.Source == nil {
		return nil, fmt.Errorf("Plan did not include Sql Select Statement?")
	}
	if m.p == nil {
		u.Debugf("custom? %v", p.Custom)
		// If we are operating in distributed mode it hasn't
		// been planned?   WE probably should allow raw data to be
		// passed via plan?
		// if _, err := m.WalkSourceSelect(nil, p); err != nil {
		// 	u.Errorf("Could not plan")
		// 	return nil, err
		// }
		m.p = p
		if p.Custom.Bool("poly_fill") {
			m.needsPolyFill = true
		}
		if partitionId := p.Custom.String("partition"); partitionId != "" {
			if p.Tbl.Partition != nil {
				for _, pt := range p.Tbl.Partition.Partitions {
					if pt.Id == partitionId {
						//u.Debugf("partition: %s   %#v", partitionId, pt)
						m.partition = pt
						if len(m.filter) == 0 {
							if pt.Left == "" {
								m.filter = bson.M{p.Tbl.Partition.Keys[0]: bson.M{"$lt": pt.Right}}
							} else if pt.Right == "" {
								m.filter = bson.M{p.Tbl.Partition.Keys[0]: bson.M{"$gte": pt.Left}}
							} else {

							}
						}
					}
				}
			}
		}
	}
	ctx := p.Context()
	m.TaskBase = exec.NewTaskBase(ctx)
	m.sel = p.Stmt.Source
	//u.Debugf("sqltomgo plan sql?  %#v", p.Stmt)
	//u.Debugf("sqltomgo plan sql.Source %#v", p.Stmt.Source)

	//filterBy, _ := json.Marshal(m.filter)
	//u.Infof("tbl %#v", m.tbl.Columns(), m.tbl)
	//u.Infof("filter: %#v  \n%s", m.filter, filterBy)
	//u.Debugf("db=%v  tbl=%v filter=%v sort=%v limit=%v skip=%v", m.schema.Name, m.tbl.Name, string(filterBy), m.sort, req.Limit, req.Offset)
	query := m.sess.DB(m.schema.Name).C(m.tbl.Name).Find(m.filter)

	//u.LogTraceDf(u.WARN, 16, "hello")
	resultReader := NewResultReader(m, query, m.limit)
	m.resp = resultReader
	//u.Debugf("sqltomgo: %p  resultreader: %p sourceplan: %p argsource:%p ", m, m.resp, m.p, p)

	return resultReader, nil
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
			// 	return m.walkUnary(curNode)
			case *expr.FuncNode:
				// All Func Nodes are Aggregates?
				esm, err := m.WalkAggs(curNode)
				if err == nil && len(esm) > 0 {
					m.aggs[col.As] = esm
				} else if err != nil {
					u.Error(err)
					return err
				}
				//u.Debugf("esm: %v:%v", col.As, esm)
				//u.Debugf(curNode.String())
			// case *expr.ArrayNode:
			// 	return m.walkArrayNode(curNode)
			// case *expr.IdentityNode:
			// 	return nil, value.NewStringValue(curNode.Text), nil
			// case *expr.StringNode:
			// 	return nil, value.NewStringValue(curNode.Text), nil
			case *expr.IdentityNode:
				//u.Debugf("likely a projection, not agg T:%T  %v", curNode, curNode)
			default:
				u.Warnf("unrecognized not agg T:%T  %v", curNode, curNode)
				//panic("Unrecognized node type")
			}
		}

	}
	return nil
}

// Group By Clause:  Mongo is a little weird where they move the
//    group by expressions INTO the aggregation clause
//
//    operation(field) FROM x GROUP BY x,y,z
//
//    db.article.aggregate([{"$group":{_id: "$author", count: {"$sum":1}}}]);
//
func (m *SqlToMgo) WalkGroupBy() error {

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
//  - For Aggregates (functions) it builds appropriate underlying mongo aggregation/map-reduce
//  - For Projections (non-functions) it does nothing, that will be done later during projection
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
	// case *expr.ArrayNode:
	// 	return m.walkArrayNode(curNode)
	// case *expr.IdentityNode:
	// 	return nil, value.NewStringValue(curNode.Text), nil
	// case *expr.StringNode:
	// 	return nil, value.NewStringValue(curNode.Text), nil
	default:
		u.Warnf("likely ?? not agg T:%T  %v", cur, cur)
		//panic("Unrecognized node type")
	}
	// if cur.Negate {
	// 	q = bson.M{"not": q}
	// }
	return q, nil
}

// Walk() an expression, and its logic to create an appropriately
//  nested bson document for mongo queries if possible.
//
// - if can't express logic we need to allow qlbridge to poly-fill
//
func (m *SqlToMgo) WalkNode(cur expr.Node, q *bson.M) (value.Value, error) {
	//u.Debugf("WalkNode: %#v", cur)
	switch curNode := cur.(type) {
	case *expr.NumberNode, *expr.StringNode:
		nodeVal, ok := vm.Eval(nil, cur)
		if !ok {
			u.Warnf("not ok %v", cur)
			return nil, fmt.Errorf("could not evaluate: %v", cur.String())
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
		return nil, fmt.Errorf("Not implemented urnary function: %v", curNode.String())
	case *expr.FuncNode:
		return m.walkFilterFunc(curNode, q)
	case *expr.IdentityNode:
		u.Warnf("we are trying to project?   %v", curNode.String())
		*q = bson.M{"terms": bson.M{"field": curNode.String()}}
		return value.NewStringValue(curNode.String()), nil
	case *expr.ArrayNode:
		return m.walkArrayNode(curNode, q)
	default:
		u.Errorf("unrecognized T:%T  %v", cur, cur)
		panic("Unrecognized node type")
	}
	// if cur.Negate {
	// 	q = bson.M{"not": q}
	// }
	return nil, nil
}

// Tri Nodes expressions:
//
//		<expression> [NOT] BETWEEN <expression> AND <expression>
//
func (m *SqlToMgo) walkFilterTri(node *expr.TriNode, q *bson.M) (value.Value, error) {

	//u.Infof("args: %#v", node.Args)
	arg1val, aok := vm.Eval(nil, node.Args[0])
	//u.Debugf("arg1? %v  ok?%v", arg1val, aok)
	arg2val, bok := vm.Eval(nil, node.Args[1])
	arg3val, cok := vm.Eval(nil, node.Args[2])
	//u.Debugf("walkTri: %v  %v %v %v", node, arg1val, arg2val, arg3val)
	if !aok || !bok || !cok {
		return nil, fmt.Errorf("Could not evaluate args: %v", node.String())
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

// Array Nodes expressions:
//
//		year IN (1990,1992)  =>
//
func (m *SqlToMgo) walkArrayNode(node *expr.ArrayNode, q *bson.M) (value.Value, error) {

	//q = bson.M{"range": bson.M{arg1val.ToString(): bson.M{"gte": arg2val.ToString(), "lte": arg3val.ToString()}}}
	terms := make([]interface{}, 0, len(node.Args))
	*q = bson.M{"$in": terms}
	for _, arg := range node.Args {
		// Do we eval here?
		v, ok := vm.Eval(nil, arg)
		if ok {
			u.Debugf("in? %T %v value=%v", v, v, v.Value())
			terms = append(terms, v.Value())
		} else {
			u.Warnf("could not evaluate arg: %v", arg)
		}
	}
	if q != nil {
		u.Debug(string(u.JsonHelper(*q).PrettyJson()))
		return nil, nil
	}
	return nil, fmt.Errorf("Uknown Error")
}

// Binary Node:   operations for >, >=, <, <=, =, !=, AND, OR, Like, IN
//
//	x = y             =>   db.users.find({field: {"$eq": value}})
//  x != y            =>   db.inventory.find( { qty: { $ne: 20 } } )
//
//  x like "list%"    =>   db.users.find( { user_id: /^list/ } )
//  x like "%list%"   =>   db.users.find( { user_id: /bc/ } )
//  x IN [a,b,c]      =>   db.users.find( { user_id: {"$in":[a,b,c] } } )
//
func (m *SqlToMgo) walkFilterBinary(node *expr.BinaryNode, q *bson.M) (value.Value, error) {

	lhval, lhok := vm.Eval(nil, node.Args[0])
	rhval, rhok := vm.Eval(nil, node.Args[1])
	if !lhok || !rhok {
		u.Warnf("not ok: %v  l:%v  r:%v", node, lhval, rhval)
		return nil, fmt.Errorf("could not evaluate: %v", node.String())
	}
	//u.Debugf("walkBinary: %v  l:%v  r:%v  %T  %T", node, lhval, rhval, lhval, rhval)
	switch node.Operator.T {
	case lex.TokenLogicAnd:
		// this doesn't yet implement x AND y AND z
		lhq, rhq := bson.M{}, bson.M{}
		_, err := m.WalkNode(node.Args[0], &lhq)
		_, err2 := m.WalkNode(node.Args[1], &rhq)
		if err != nil || err2 != nil {
			u.Errorf("could not get children nodes? %v %v %v", err, err2, node)
			return nil, fmt.Errorf("could not evaluate: %v", node.String())
		}
		*q = bson.M{"$and": []bson.M{lhq, rhq}}
	case lex.TokenLogicOr:
		// this doesn't yet implement x AND y AND z
		lhq, rhq := bson.M{}, bson.M{}
		_, err := m.WalkNode(node.Args[0], &lhq)
		_, err2 := m.WalkNode(node.Args[1], &rhq)
		if err != nil || err2 != nil {
			u.Errorf("could not get children nodes? %v %v %v", err, err2, node)
			return nil, fmt.Errorf("could not evaluate: %v", node.String())
		}
		*q = bson.M{"$or": []bson.M{lhq, rhq}}
	case lex.TokenEqual, lex.TokenEqualEqual:
		// The $eq expression is equivalent to { field: <value> }.
		if lhval != nil && rhval != nil {
			*q = bson.M{lhval.ToString(): rhval.Value()}
			//u.Infof("m=%#v type=%v", q, rhval.Type())
			return nil, nil
		}
		if lhval != nil || rhval != nil {
			u.Infof("has stuff?  %v", node.String())
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
		//u.Infof("LIKE:  %v  startsPCt?%v  endsPct?%v", rhs, startsPct, endsPct)
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

	case lex.TokenIN:
		switch vt := rhval.(type) {
		case value.SliceValue:
			*q = bson.M{lhval.ToString(): bson.M{"$in": vt.Values()}}
		default:
			u.Warnf("not implemented type %#v", rhval)
		}

	default:
		u.Warnf("not implemented: %v", node.Operator)
	}
	if q != nil {
		return nil, nil
	}
	return nil, fmt.Errorf("not implemented %v", node.String())
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
		switch nt := node.Args[0].(type) {
		case *expr.IdentityNode:
			fieldName = nt.String()
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
//   doesn't really exist, then map that function to an Mongo Aggregation/MapReduce function
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
			//u.Debugf("not able to run as native mongo query, running polyfill: %s", node.String())
			//return nil, fmt.Errorf("Invalid func")
		}
		val, ok := eval(node.Args[0])
		if !ok {
			//u.Warnf("Could not run node as mongo: %v", node.String())
			m.needsPolyFill = true
		} else {
			// "min_price" : { "min" : { "field" : "price" } }
			q = bson.M{funcName: bson.M{"field": val.ToString()}}
		}
		return q, nil
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
		//u.Warnf("how do we want to use count(*)?  ?")
		val, ok := eval(node.Args[0])
		if !ok {
			u.Errorf("Must be valid: %v", node.String())
			return nil, fmt.Errorf("Invalid argument: %v", node.String())
		}
		if val.ToString() == "*" {
			//return nil, nil
			return bson.M{"$sum": 2}, nil
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
		//u.Errorf("unrecognized T:%T  %v", cur, cur)
	}
	return value.NilValueVal, false
}
