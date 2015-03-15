package models

import (
	"database/sql/driver"
	"strings"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/value"
	"github.com/dataux/dataux/vendor/mixer/mysql"
)

var _ = u.EMPTY

// Table represents traditional definition of Database Table
//   It belongs to a Schema and can be used to
//   create a Datasource used to read this table
type Table struct {
	Name           string                  // Name of table
	FieldPositions map[string]int          // Position of field name to column list
	Fields         []*Field                // List of Fields, in order
	FieldMap       map[string]*Field       // List of Fields, in order
	FieldsMySql    []*mysql.Field          // Mysql types
	FieldMapMySql  map[string]*mysql.Field // shortcut for pre-build Mysql types
	DescribeValues [][]driver.Value        // The Values that will be output for Describe
	Schema         *Schema                 // The schema this is member of
	Charset        uint16                  // Character set, default = utf8
}

// Schema is a collection of tables/datatypes, servers to be
//  a single schema.  Must be same BackendType (mysql, elasticsearch)
type Schema struct {
	Db         string
	SourceType string `json:"source_type"`
	Address    string `json:"address"` // If you have don't need per node routing
	Nodes      map[string]*SourceConfig
	Conf       *SchemaConfig
	Tables     map[string]*Table
	TableNames []string
	DataSource DataSource
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

// func (m *Schema) AddTable(name string, fields map[string]value.ValueType) {
// 	name = strings.ToLower(name)
// 	if _, ok := m.Tables[name]; ok {
// 		u.Warnf("Already had table: %v", name)
// 	} else {
// 		m.TableNames = append(m.TableNames, name)
// 		sort.Strings(m.TableNames)
// 	}
// 	t := Table{Fields: fields, Schema: m}
// 	m.Tables[name] = &t
// }

// Get a backend to fulfill a request
func (m *Schema) ChooseBackend() string {
	if m.Address != "" {
		return m.Address
	}
	// ELSE:   round-robbin?   hostpool?
	return m.Address
}

func (m *Schema) Table(tableName string) (*Table, error) {
	tbl := m.Tables[tableName]
	if tbl != nil {
		return tbl, nil
	}
	return m.DataSource.Table(tableName)
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
