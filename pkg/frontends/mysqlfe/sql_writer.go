package mysqlfe

import (
	"bytes"
	"fmt"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/schema"
	"github.com/araddon/qlbridge/value"
)

var _ = u.EMPTY

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
