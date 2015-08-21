package backends

import (
	"database/sql/driver"
	"fmt"
	"strings"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/datasource/membtree"
	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"

	"github.com/dataux/dataux/pkg/models"
)

var (
	_ = u.EMPTY
)

func (m *Builder) emptyTask(name string) (exec.Tasks, error) {
	source := membtree.NewStaticDataSource(name, 0, nil, []string{name})
	m.Projection = expr.NewProjection()
	m.Projection.AddColumnShort(name, value.StringType)
	tasks := make(exec.Tasks, 0)
	sourceTask := exec.NewSource(nil, source)
	tasks.Add(sourceTask)
	return tasks, nil
}
func (m *Builder) VisitShow(stmt *expr.SqlShow) (interface{}, error) {
	u.Debugf("VisitShow %q  %s", stmt.Identity, stmt.Raw)

	raw := strings.ToLower(stmt.Raw)
	switch {
	case strings.ToLower(stmt.Identity) == "variables":
		// SHOW variables;
		vals := make([][]driver.Value, 2)
		vals[0] = []driver.Value{"auto_increment_increment", "1"}
		vals[1] = []driver.Value{"collation", "utf8"}
		source := membtree.NewStaticDataSource("variables", 0, vals, []string{"Variable_name", "Value"})
		m.Projection = expr.NewProjection()
		m.Projection.AddColumnShort("Variable_name", value.StringType)
		m.Projection.AddColumnShort("Value", value.StringType)
		tasks := make(exec.Tasks, 0)
		sourceTask := exec.NewSource(nil, source)
		u.Infof("source:  %#v", source)
		tasks.Add(sourceTask)
		return tasks, nil
	case strings.ToLower(stmt.Identity) == "databases":
		// SHOW databases;
		vals := make([][]driver.Value, 1)
		vals[0] = []driver.Value{m.schema.Name}
		source := membtree.NewStaticDataSource("databases", 0, vals, []string{"Database"})
		m.Projection = expr.NewProjection()
		m.Projection.AddColumnShort("Database", value.StringType)
		tasks := make(exec.Tasks, 0)
		sourceTask := exec.NewSource(nil, source)
		u.Infof("source:  %#v", source)
		tasks.Add(sourceTask)
		return tasks, nil
	case strings.ToLower(stmt.Identity) == "collation":
		// SHOW collation;
		vals := make([][]driver.Value, 1)
		// utf8_general_ci          | utf8     |  33 | Yes     | Yes      |       1 |
		vals[0] = []driver.Value{"utf8_general_ci", "utf8", 33, "Yes", "Yes", 1}
		cols := []string{"Collation", "Charset", "Id", "Default", "Compiled", "Sortlen"}
		source := membtree.NewStaticDataSource("collation", 0, vals, cols)
		m.Projection = expr.NewProjection()
		m.Projection.AddColumnShort("Collation", value.StringType)
		m.Projection.AddColumnShort("Charset", value.StringType)
		m.Projection.AddColumnShort("Id", value.IntType)
		m.Projection.AddColumnShort("Default", value.StringType)
		m.Projection.AddColumnShort("Compiled", value.StringType)
		m.Projection.AddColumnShort("Sortlen", value.IntType)
		tasks := make(exec.Tasks, 0)
		sourceTask := exec.NewSource(nil, source)
		u.Infof("source:  %#v", source)
		tasks.Add(sourceTask)
		return tasks, nil
	case strings.HasPrefix(raw, "show session"):
		//SHOW SESSION VARIABLES LIKE 'lower_case_table_names';
		source, proj := models.ShowVariables(m.schema, "lower_case_table_names", 0)
		m.Projection = proj
		tasks := make(exec.Tasks, 0)
		sourceTask := exec.NewSource(nil, source)
		u.Infof("source:  %#v", source)
		tasks.Add(sourceTask)
		return tasks, nil
	case strings.ToLower(stmt.Identity) == "tables" || strings.ToLower(stmt.Identity) == m.schema.Name:
		if stmt.Full {
			u.Debugf("show tables: %+v", m.schema)
			tables := m.schema.Tables()
			vals := make([][]driver.Value, len(tables))
			row := 0
			for _, tbl := range tables {
				vals[row] = []driver.Value{tbl, "BASE TABLE"}
				row++
			}
			source := membtree.NewStaticDataSource("tables", 0, vals, []string{"Tables", "Table_type"})
			m.Projection = expr.NewProjection()
			m.Projection.AddColumnShort("Tables", value.StringType)
			m.Projection.AddColumnShort("Table_type", value.StringType)
			tasks := make(exec.Tasks, 0)
			sourceTask := exec.NewSource(nil, source)
			u.Infof("source:  %#v", source)
			tasks.Add(sourceTask)
			return tasks, nil
		}
		// SHOW TABLES;
		//u.Debugf("show tables: %+v", m.schema)
		source, proj := models.ShowTables(m.schema)
		m.Projection = proj
		tasks := make(exec.Tasks, 0)
		sourceTask := exec.NewSource(nil, source)
		//u.Infof("source:  %#v", source)
		tasks.Add(sourceTask)
		return tasks, nil
	case strings.ToLower(stmt.Identity) == "procedure":
		// SHOW PROCEDURE STATUS WHERE Db='mydb'
		return m.emptyTask("Procedures")
	case strings.ToLower(stmt.Identity) == "function":
		// SHOW FUNCTION STATUS WHERE Db='mydb'
		return m.emptyTask("Function")
	default:
		// SHOW FULL TABLES FROM `auths`
		desc := expr.SqlDescribe{}
		desc.Identity = stmt.Identity
		return m.VisitDescribe(&desc)
	}
	return nil, fmt.Errorf("No handler found")
}

func (m *Builder) VisitDescribe(stmt *expr.SqlDescribe) (interface{}, error) {
	u.Debugf("VisitDescribe %+v", stmt)

	if m.schema == nil {
		return nil, ErrNoSchemaSelected
	}
	tbl, err := m.schema.Table(strings.ToLower(stmt.Identity))
	if err != nil {
		u.Errorf("could not get table: %v", err)
		return nil, err
	}
	source, proj := models.DescribeTable(tbl)
	m.Projection = proj

	tasks := make(exec.Tasks, 0)
	sourceTask := exec.NewSource(nil, source)
	u.Infof("source:  %#v", source)
	tasks.Add(sourceTask)

	return tasks, nil
}
