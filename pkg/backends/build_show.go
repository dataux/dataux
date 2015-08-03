package backends

import (
	"database/sql/driver"
	"fmt"
	"strings"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/datasource/inmemmap"
	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
)

var (
	_ = u.EMPTY
)

func (m *Builder) emptyTask(name string) (exec.Tasks, error) {
	source := inmemmap.NewStaticDataSource(name, 0, nil, []string{name})
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
	case strings.ToLower(stmt.Identity) == "databases":
		// SHOW databases;
		vals := make([][]driver.Value, 1)
		vals[0] = []driver.Value{m.schema.Name}
		source := inmemmap.NewStaticDataSource("databases", 0, vals, []string{"Database"})
		m.Projection = expr.NewProjection()
		m.Projection.AddColumnShort("Database", value.StringType)
		tasks := make(exec.Tasks, 0)
		sourceTask := exec.NewSource(nil, source)
		u.Infof("source:  %#v", source)
		tasks.Add(sourceTask)
		return tasks, nil
	case strings.HasPrefix(raw, "show session"):
		//SHOW SESSION VARIABLES LIKE 'lower_case_table_names';
		source, proj := m.schema.ShowVariables("lower_case_table_names", 0)
		m.Projection = proj
		tasks := make(exec.Tasks, 0)
		sourceTask := exec.NewSource(nil, source)
		u.Infof("source:  %#v", source)
		tasks.Add(sourceTask)
		return tasks, nil
	case strings.ToLower(stmt.Identity) == "tables" || strings.ToLower(stmt.Identity) == m.schema.Name:
		if stmt.Full {
			vals := make([][]driver.Value, len(m.schema.TableNames))
			row := 0
			for _, tbl := range m.schema.Tables {
				vals[row] = []driver.Value{tbl.Name, "BASE TABLE"}
				row++
			}
			source := inmemmap.NewStaticDataSource("tables", 0, vals, []string{"Tables", "Table_type"})
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
		source, proj := m.schema.ShowTables()
		m.Projection = proj
		tasks := make(exec.Tasks, 0)
		sourceTask := exec.NewSource(nil, source)
		u.Infof("source:  %#v", source)
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
	source, proj := tbl.DescribeTable()
	m.Projection = proj

	tasks := make(exec.Tasks, 0)
	sourceTask := exec.NewSource(nil, source)
	u.Infof("source:  %#v", source)
	tasks.Add(sourceTask)

	return tasks, nil
}
