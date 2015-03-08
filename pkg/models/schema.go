package models

import (
	"sort"
	"strings"

	u "github.com/araddon/gou"
	"github.com/dataux/dataux/vendor/mixer/mysql"
)

// Table represents traditional definition of Database Table
//   It belongs to a Schema and can be used to
//   create a Datasource used to read this table
type Table struct {
	Name           string
	Fields         []*mysql.Field
	FieldMap       map[string]*mysql.Field
	DescribeValues [][]interface{}
}

// Schema is a collection of tables/datatypes, servers to be
//  a single schema.  Must be same BackendType (mysql, elasticsearch)
type Schema struct {
	Db          string
	BackendType string `json:"backend_type"`
	Address     string `json:"address"` // If you have don't need per node routing
	Nodes       map[string]*BackendConfig
	Conf        *SchemaConfig
	Tables      map[string]*Table
	TableNames  []string
}

func (m *Schema) AddTable(name string, fields []*mysql.Field) {
	name = strings.ToLower(name)
	if _, ok := m.Tables[name]; ok {
		u.Warnf("Already had table: %v", name)
	} else {
		m.TableNames = append(m.TableNames, name)
		sort.Strings(m.TableNames)
	}
	t := Table{Fields: fields}
	m.Tables[name] = &t
}

// Get a backend to fulfill a request
func (m *Schema) ChooseBackend() string {
	if m.Address != "" {
		return m.Address
	}
	// ELSE:   round-robbin?   hostpool?
	return m.Address
}

func NewTable(table string) *Table {
	t := &Table{
		Name:     strings.ToLower(table),
		FieldMap: make(map[string]*mysql.Field),
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

func (m *Table) AddValues(values []interface{}) {
	m.DescribeValues = append(m.DescribeValues, values)
	//rowData, _ := mysql.ValuesToRowData(values, r.Fields)
	//r.RowDatas = append(r.RowDatas, rowData)
}
func (m *Table) AddField(fld *mysql.Field) {
	m.Fields = append(m.Fields, fld)
	if fld.FieldName == "" {
		fld.FieldName = string(fld.Name)
	}
	m.FieldMap[fld.FieldName] = fld
}
func (m *Table) FieldNames() map[string]int {
	names := make(map[string]int)
	for i, f := range m.Fields {
		names[string(f.Name)] = i
	}
	return names
}
