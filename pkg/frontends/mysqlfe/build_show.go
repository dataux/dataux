package mysqlfe

import (
	"database/sql/driver"
	"fmt"
	"strings"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/datasource/membtree"
	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/plan"
	"github.com/araddon/qlbridge/value"
)

var (
	_ = u.EMPTY
)

// Many of the ShowMethods are MySql dialect specific so will be replaced here
func (m *MySqlJob) VisitShow(stmt *expr.SqlShow) (expr.Task, expr.VisitStatus, error) {

	u.Debugf("mysql.VisitShow create?%v  identity=%q  raw=%s", stmt.Create, stmt.Identity, stmt.Raw)

	//raw := strings.ToLower(stmt.Raw)
	switch {
	case stmt.Create && strings.ToLower(stmt.CreateWhat) == "table":
		// SHOW CREATE TABLE

		// _, tableName, _ := expr.LeftRight(stmt.Identity)
		// tableLower := strings.ToLower(tableName)
		tbl, _ := m.Ctx.Schema.Table(stmt.Identity)
		if tbl == nil {
			u.Warnf("no table? %q", stmt.Identity)
			return nil, expr.VisitError, fmt.Errorf("No table found for %q", stmt.Identity)
		}
		// Get the create table statement
		createStmt, err := TableCreate(tbl)
		if err != nil {
			return nil, expr.VisitError, err
		}
		rows := make([][]driver.Value, 1)
		rows[0] = []driver.Value{tbl.Name, createStmt}
		source := membtree.NewStaticDataSource("tables", 0, rows, []string{"Table", "Create Table"})
		proj := expr.NewProjection()
		proj.AddColumnShort("Table", value.StringType)
		proj.AddColumnShort("Create Table", value.StringType)
		m.Ctx.Projection = plan.NewProjectionStatic(proj)
		tasks := make(exec.Tasks, 0)
		sourceTask := exec.NewSource(m.Ctx, nil, source)
		tasks.Add(sourceTask)
		return exec.NewSequential(m.Ctx, "show-tables", tasks), expr.VisitContinue, nil
	}
	return m.Visitor.VisitShow(stmt)
}
