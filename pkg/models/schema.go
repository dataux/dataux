package models

import (
	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/schema"
	"github.com/araddon/qlbridge/value"

	"github.com/dataux/dataux/vendored/mixer/mysql"
)

var (
	_ = u.EMPTY
)

// func TableToMysqlResultset(tbl *schema.Table) *mysql.Resultset {
// 	rs := new(mysql.Resultset)
// 	rs.Fields = mysql.DescribeHeaders
// 	rs.FieldNames = mysql.DescribeFieldNames
// 	for _, val := range tbl.DescribeValues {
// 		rs.AddRowValues(val)
// 	}
// 	return rs
// }

func FieldToMysql(f *schema.Field, s *schema.SourceSchema) *mysql.Field {
	switch f.Type {
	case value.StringType:
		return mysql.NewField(f.Name, s.Name, s.Name, f.Length, mysql.MYSQL_TYPE_STRING)
	case value.BoolType:
		return mysql.NewField(f.Name, s.Name, s.Name, 1, mysql.MYSQL_TYPE_TINY)
	case value.IntType:
		return mysql.NewField(f.Name, s.Name, s.Name, 32, mysql.MYSQL_TYPE_LONG)
	case value.NumberType:
		return mysql.NewField(f.Name, s.Name, s.Name, 64, mysql.MYSQL_TYPE_FLOAT)
	case value.TimeType:
		return mysql.NewField(f.Name, s.Name, s.Name, 8, mysql.MYSQL_TYPE_DATETIME)
	default:
		u.Warnf("Could not find mysql type for :%T", f.Type)
	}

	return nil
}
