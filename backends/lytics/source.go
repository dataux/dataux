package lytics

import (
	"fmt"
	"os"
	"strings"

	u "github.com/araddon/gou"
	lytics "github.com/lytics/go-lytics"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/schema"
	"github.com/araddon/qlbridge/value"
)

var (
	// implement interfaces
	_ schema.Source = (*Source)(nil)
)

const (
	// SourceType defines the "source" type from qlbridge datasource
	SourceType = "lytics"
)

func init() {
	// We need to register our DataSource provider here
	datasource.Register(SourceType, &Source{})
}

// Source is the Lytics data source provider
// responsible for schema management, and connection
// management to Lytics
type Source struct {
	srcschema *schema.SchemaSource
	conf      *schema.ConfigSource
	tables    []string // lower cased
	tablemap  map[string]*schema.Table
	apiKey    string
	client    *lytics.Client
}

func (m *Source) Init() {}

func (m *Source) Setup(ss *schema.SchemaSource) error {

	if m.srcschema != nil {
		return nil
	}

	m.srcschema = ss
	m.conf = ss.Conf
	m.tablemap = make(map[string]*schema.Table)

	u.Infof("%p Conf: %+v  settings:%v", ss, ss.Conf, ss.Conf.Settings)
	if ss.Conf != nil && len(ss.Conf.Settings) > 0 {
		m.apiKey = ss.Conf.Settings.String("apikey")
	}
	if m.apiKey == "" {
		m.apiKey = os.Getenv("LIOKEY")
	}
	if m.apiKey == "" {
		return fmt.Errorf(`Requires Lytics "apikey"`)
	}

	m.client = lytics.NewLytics(m.apiKey, "", nil)
	u.Debugf("Init() Lytics schema P=%p", m.srcschema)

	if err := m.loadSchema(); err != nil {
		u.Errorf("could not load es tables: %v", err)
		return err
	}
	if m.srcschema != nil {
		u.Debugf("Post Init() Lytics schema P=%p tblct=%d", m.srcschema, len(m.srcschema.Tables()))
	}
	return nil
}

func (m *Source) Open(schemaName string) (schema.Conn, error) {
	u.Debugf("Open(%v)", schemaName)
	tbl, err := m.srcschema.Table(schemaName)
	if err != nil {
		return nil, err
	}
	if tbl == nil {
		u.Errorf("Could not find table for '%s'.'%s'", m.srcschema.Name, schemaName)
		return nil, fmt.Errorf("Could not find '%v'.'%v' schema", m.srcschema.Name, schemaName)
	}

	sqlConverter := NewGenerator(tbl, m.apiKey)
	return sqlConverter, nil
}

func (m *Source) Close() error              { return nil }
func (m *Source) DataSource() schema.Source { return m }

func (m *Source) Tables() []string { return m.tables }

func (m *Source) Table(table string) (*schema.Table, error) {
	//u.Debugf("get table for %s", table)
	t := m.tablemap[table]
	if t != nil {
		return t, nil
	}
	tlower := strings.ToLower(table)
	t = m.tablemap[tlower]
	if t != nil {
		return t, nil
	}
	return nil, schema.ErrNotFound
}

func (m *Source) loadSchema() error {

	schemas, err := m.client.GetSchema()
	if err != nil {
		return err
	}

	m.tables = make([]string, 0, len(schemas))

	for _, s := range schemas {
		m.tables = append(m.tables, s.Name)
		if err = m.loadTableSchema(s); err != nil {
			return err
		}
	}
	u.Debugf("found tables: %v", m.tables)
	return nil
}

func (m *Source) loadTableSchema(s *lytics.Schema) error {

	tbl := schema.NewTable(s.Name)

	for _, col := range s.Columns {

		u.Infof("%#v", col)

		var fld *schema.Field
		switch col.Type {
		case "boolean", "bool":
			fld = schema.NewFieldBase(col.As, value.BoolType, 1, col.ShortDesc)
		case "string":
			fld = schema.NewFieldBase(col.As, value.StringType, 255, col.ShortDesc)
		case "date":
			fld = schema.NewFieldBase(col.As, value.TimeType, 32, col.ShortDesc)
		case "int", "long", "integer":
			fld = schema.NewFieldBase(col.As, value.IntType, 46, col.ShortDesc)
		case "double", "float", "number":
			fld = schema.NewFieldBase(col.As, value.NumberType, 64, col.ShortDesc)
		case "[]string", "ts[]string":
			fld = schema.NewFieldBase(col.As, value.StringsType, 2000, col.ShortDesc)
		case "map[string]intsum", "map[string]int", "map[string]number",
			"map[string]time", "map[string]string", "map[string]bool", "membership":
			fld = schema.NewFieldBase(col.As, value.JsonType, 2000, col.ShortDesc)
		case "[]timebucket", "dynamic":
			continue
		default:
			u.Warnf("Unahndled type %v type=%v", col.As, col.Type)
		}
		if fld != nil {
			tbl.AddField(fld)
		}

	}

	keys := make([]string, len(tbl.Fields))
	for i, f := range tbl.Fields {
		keys[i] = f.Name
	}
	tbl.SetColumns(keys)
	m.tablemap[tbl.Name] = tbl

	return nil
}
