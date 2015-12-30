package mysqlfe

import (
	"bytes"
	"database/sql/driver"
	"fmt"
	"strings"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/schema"
	"github.com/araddon/qlbridge/value"
)

var _ = u.EMPTY

func typeToMysql(f *schema.Field) string {
	// char(60)
	// varchar(255)
	// text
	switch f.Type {
	case value.IntType:
		if f.Length == 64 {
			return "bigint"
		} else if f.Length == 0 {
			return "int(32)"
		}
		return fmt.Sprintf("int(%d)", f.Length)
	case value.NumberType:
		if f.Length == 64 {
			return "float"
		} else if f.Length == 0 {
			return "float"
		}
		return "float"
	case value.BoolType:
		return "boolean"
	case value.TimeType:
		return "datetime"
	case value.StringType:
		if f.Length != 0 {
			return fmt.Sprintf("varchar(%d)", f.Length)
		}
		return "varchar(255)"
	}
	return "text"
}
func fieldDescribe(proj *expr.Projection, f *schema.Field) []driver.Value {

	null := "YES"
	if f.NoNulls {
		null = "NO"
	}
	if len(proj.Columns) == 6 {
		//[]string{"Field", "Type",  "Null", "Key", "Default", "Extra"}
		if f.Name == "_id" {
			u.Debugf("nulls? %v", f.NoNulls)
		}
		return []driver.Value{
			f.Name,
			typeToMysql(f),
			null,
			f.Key,
			f.DefaultValue,
			f.Description,
		}
	}
	privileges := ""
	if len(f.Roles) > 0 {
		privileges = fmt.Sprintf("{%s}", strings.Join(f.Roles, ", "))
	}
	//[]string{"Field", "Type", "Collation", "Null", "Key", "Default", "Extra", "Privileges", "Comment"}
	return []driver.Value{
		f.Name,
		typeToMysql(f),
		"", // collation
		null,
		f.Key,
		f.DefaultValue,
		f.Extra,
		privileges,
		f.Description,
	}
}

// Implement Dialect Specific Writers
//     ie, mysql, postgres, cassandra all have different dialects
//     so the Create statements are quite different

// Take a table and make create statement
func TableCreate(tbl *schema.Table) (string, error) {

	w := &bytes.Buffer{}
	fmt.Fprintf(w, "CREATE TABLE `%s` (", tbl.Name)
	for i, fld := range tbl.Fields {
		if i != 0 {
			w.WriteByte(',')
		}
		fmt.Fprint(w, "\n    ")
		writeField(w, fld)
	}
	fmt.Fprint(w, "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8")
	//tblStr := fmt.Sprintf("CREATE TABLE `%s` (\n\n);", tbl.Name, strings.Join(cols, ","))
	//return tblStr, nil
	return w.String(), nil
}
func writeField(w *bytes.Buffer, fld *schema.Field) {
	fmt.Fprintf(w, "`%s` ", fld.Name)
	deflen := fld.Length
	switch fld.Type {
	case value.BoolType:
		fmt.Fprint(w, "tinyint(1) DEFAULT NULL")
	case value.IntType:
		fmt.Fprint(w, "bigint DEFAULT NULL")
	case value.StringType:
		if deflen == 0 {
			deflen = 255
		}
		fmt.Fprintf(w, "varchar(%d) DEFAULT NULL", deflen)
	case value.NumberType:
		fmt.Fprint(w, "float DEFAULT NULL")
	case value.TimeType:
		fmt.Fprint(w, "datetime DEFAULT NULL")
	default:
		fmt.Fprint(w, "text DEFAULT NULL")
	}
	if len(fld.Description) > 0 {
		fmt.Fprintf(w, " COMMENT %q", fld.Description)
	}
}
