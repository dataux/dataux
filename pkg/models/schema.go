package models

import (
	"database/sql/driver"
	"strings"
	"time"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
	"github.com/dataux/dataux/vendor/mixer/mysql"
)

var (
	_ = u.EMPTY

	// Standard headers
	DescribeHeaders = NewDescribeHeaders()

	// Schema Refresh Interval
	SchemaRefreshInterval = -time.Minute * 1
)

func NewDescribeHeaders() []*Field {
	fields := make([]*Field, 6)
	fields[0] = NewField("Field", value.StringType, 255, "COLUMN_NAME")
	fields[1] = NewField("Type", value.StringType, 32, "COLUMN_TYPE")
	fields[2] = NewField("Null", value.StringType, 4, "IS_NULLABLE")
	fields[3] = NewField("Key", value.StringType, 64, "COLUMN_KEY")
	fields[4] = NewField("Default", value.StringType, 32, "COLUMN_DEFAULT")
	fields[5] = NewField("Extra", value.StringType, 255, "EXTRA")
	return fields
}

// Table represents traditional definition of Database Table
//   It belongs to a Schema and can be used to
//   create a Datasource used to read this table
type Table struct {
	Name            string                  // Name of table
	FieldPositions  map[string]int          // Position of field name to column list
	Fields          []*Field                // List of Fields, in order
	FieldMap        map[string]*Field       // List of Fields, in order
	FieldsMySql     []*mysql.Field          // Mysql types
	FieldMapMySql   map[string]*mysql.Field // shortcut for pre-build Mysql types
	DescribeValues  [][]driver.Value        // The Values that will be output for Describe
	Schema          *Schema                 // The schema this is member of
	Charset         uint16                  // Character set, default = utf8
	lastRefreshed   time.Time               // Last time we refreshed this schema
	tableProjection *expr.Projection
	tableVals       *datasource.StaticDataSource
}

// Schema is a "Virtual" Database.  A collection of tables/datatypes, servers to be
//  a single schema.  Must be same BackendType (mysql, elasticsearch)
type Schema struct {
	Db                  string                   `json:"db"`          // Virtual DB = Schema
	SourceType          string                   `json:"source_type"` // [mysql,elasticsearch, csv, etc]
	Address             string                   `json:"address"`     // If you have don't need per node routing
	Nodes               map[string]*SourceConfig // map of Servers/Nodes to its config
	Conf                *SchemaConfig            // Schema level configuration
	Tables              map[string]*Table        // Tables in this schema
	TableNames          []string                 // Table name
	DataSource          DataSource               // The datasource Interface
	lastRefreshed       time.Time                // Last time we refreshed this schema
	showTableProjection *expr.Projection
	showTableVals       *datasource.StaticDataSource
}

type FieldData []byte

// Field is a Descriptor of a Field/Column within a table
type Field struct {
	Name               string
	Description        string
	Data               FieldData
	Length             uint32
	Type               value.ValueType
	DefaultValueLength uint64
	DefaultValue       []byte
	Indexed            bool
}

func NewField(name string, valType value.ValueType, size int, description string) *Field {
	return &Field{
		Name:        name,
		Description: description,
		Length:      uint32(size),
		Type:        valType,
	}
}

func (m *Field) ToMysql(s *Schema) *mysql.Field {
	switch m.Type {
	case value.StringType:
		return mysql.NewField(m.Name, s.Db, s.Db, m.Length, mysql.MYSQL_TYPE_STRING)
	case value.BoolType:
		return mysql.NewField(m.Name, s.Db, s.Db, 1, mysql.MYSQL_TYPE_TINY)
	case value.IntType:
		return mysql.NewField(m.Name, s.Db, s.Db, 32, mysql.MYSQL_TYPE_LONG)
	case value.NumberType:
		return mysql.NewField(m.Name, s.Db, s.Db, 64, mysql.MYSQL_TYPE_FLOAT)
	case value.TimeType:
		return mysql.NewField(m.Name, s.Db, s.Db, 8, mysql.MYSQL_TYPE_DATETIME)
	default:
		u.Warnf("Could not find mysql type for :%T", m.Type)
	}

	return nil
}

// Get a backend to fulfill a request
func (m *Schema) ChooseBackend() string {
	if m.Address != "" {
		return m.Address
	}
	// ELSE:   round-robbin?   hostpool?
	return m.Address
}

func (m *Schema) ShowTables() (*datasource.StaticDataSource, *expr.Projection) {

	if m.showTableVals == nil {
		vals := make([][]driver.Value, len(m.TableNames))
		idx := 0
		if len(m.TableNames) == 0 {
			u.Warnf("NO TABLES!!!!! for %s p=%p", m.Db, m)
		}
		for _, tbl := range m.TableNames {
			vals[idx] = []driver.Value{tbl}
			//u.Infof("found table: %v   vals=%v", tbl, vals[idx])
			idx++
		}
		m.showTableVals = datasource.NewStaticDataSource("schematables", vals, []string{"Table"})
		p := expr.NewProjection()
		p.AddColumnShort("Table", value.StringType)
		m.showTableProjection = p
	}
	//u.Infof("showtables:  %v", m.showTableVals)
	return m.showTableVals, m.showTableProjection
}

func (m *Schema) Table(tableName string) (*Table, error) {
	tbl := m.Tables[tableName]
	if tbl != nil {
		return tbl, nil
	}
	return m.DataSource.Table(tableName)
}

// Is this schema object current?
func (m *Schema) Current() bool {
	return m.Since(SchemaRefreshInterval)
}

// Is this schema object within time window described by @dur time ago ?
func (m *Schema) Since(dur time.Duration) bool {
	if m.lastRefreshed.IsZero() {
		return false
	}
	if m.lastRefreshed.After(time.Now().Add(dur)) {
		return true
	}
	return false
}

func NewTable(table string, s *Schema) *Table {
	t := &Table{
		Name:          strings.ToLower(table),
		Fields:        make([]*Field, 0),
		FieldMap:      make(map[string]*Field),
		Schema:        s,
		FieldMapMySql: make(map[string]*mysql.Field),
	}
	return t
}

func (m *Table) DescribeResultset() *mysql.Resultset {
	rs := new(mysql.Resultset)
	rs.Fields = mysql.DescribeHeaders
	rs.FieldNames = mysql.DescribeFieldNames
	for _, val := range m.DescribeValues {
		rs.AddRowValues(val)
	}
	return rs
}

func (m *Table) DescribeTable() (*datasource.StaticDataSource, *expr.Projection) {

	//tbl.AddField(models.NewField("_score", value.NumberType, 24, "Created per Search By Elasticsearch"))
	//tbl.AddValues([]driver.Value{"_id", "string", "NO", "PRI", "AUTOGEN", ""})

	if m.tableVals == nil {
		if len(m.Fields) == 0 {
			u.Warnf("NO Fields!!!!! for %s p=%p", m.Name, m)
		}
		p := expr.NewProjection()
		for _, f := range DescribeHeaders {
			p.AddColumnShort(string(f.Name), f.Type)
			u.Infof("found field:  vals=%#v", f)
		}
		m.tableVals = datasource.NewStaticDataSource("describetable", m.DescribeValues, nil)
		m.tableProjection = p
	}
	u.Infof("describe table:  %v", m.tableVals)
	return m.tableVals, m.tableProjection
}

func (m *Table) HasField(name string) bool {
	if _, ok := m.FieldMap[name]; ok {
		return true
	}
	return false
}

func (m *Table) AddValues(values []driver.Value) {
	m.DescribeValues = append(m.DescribeValues, values)
	//rowData, _ := mysql.ValuesToRowData(values, r.Fields)
	//r.RowDatas = append(r.RowDatas, rowData)
}

func (m *Table) AddField(fld *Field) {
	m.Fields = append(m.Fields, fld)
	m.FieldMap[fld.Name] = fld
	mySqlFld := mysql.NewField(fld.Name, m.Schema.Db, m.Schema.Db, fld.Length, mysql.MYSQL_TYPE_STRING)
	m.AddMySqlField(mySqlFld)
}

func (m *Table) AddFieldType(name string, valType value.ValueType) {
	m.AddField(&Field{Type: valType, Name: name})
}

func (m *Table) AddMySqlField(fld *mysql.Field) {
	m.FieldsMySql = append(m.FieldsMySql, fld)
	if fld.FieldName == "" {
		fld.FieldName = string(fld.Name)
	}
	m.FieldMapMySql[fld.FieldName] = fld
}

// List of Field Names and ordinal position in Column list
func (m *Table) FieldNamesPositions() map[string]int {
	return m.FieldPositions
}

// Is this schema object current?
func (m *Table) Current() bool {
	return m.Since(SchemaRefreshInterval)
}

// Is this schema object within time window described by @dur time ago ?
func (m *Table) Since(dur time.Duration) bool {
	if m.lastRefreshed.IsZero() {
		return false
	}
	if m.lastRefreshed.After(time.Now().Add(dur)) {
		return true
	}
	return false
}
