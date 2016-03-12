package files

import (
	"fmt"
	"os"
	"strings"
	"time"

	u "github.com/araddon/gou"
	"github.com/lytics/cloudstorage"
	"github.com/lytics/cloudstorage/logging"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/schema"
	//"github.com/araddon/qlbridge/value"
)

/*
TODO:
  - Pooled Connections?  we should have more than one




*/
var (
	// implement interfaces
	_ schema.DataSource = (*FileSource)(nil)

	schemaRefreshInterval = time.Minute * 5
)

const (
	SourceType = "cloudstore"
)

func init() {
	// We need to register our DataSource provider here
	datasource.Register(SourceType, NewFileSource())
}

var _ = u.EMPTY

var localconfig = &cloudstorage.CloudStoreContext{
	LogggingContext: "unittest",
	TokenSource:     cloudstorage.LocalFileSource,
	LocalFS:         "/tmp/mockcloud",
	TmpDir:          "/tmp/localcache",
}

var gcsIntconfig = &cloudstorage.CloudStoreContext{
	LogggingContext: "integration-test",
	TokenSource:     cloudstorage.GCEDefaultOAuthToken,
	Project:         "lytics-dev",
	Bucket:          "lytics-dataux-tests",
	TmpDir:          "/tmp/localcache",
}

func CreateStore() (cloudstorage.Store, error) {

	cloudstorage.LogConstructor = func(prefix string) logging.Logger {
		return logging.NewStdLogger(true, logging.DEBUG, prefix)
	}

	var config *cloudstorage.CloudStoreContext
	if os.Getenv("TESTINT") == "" {
		//os.RemoveAll("/tmp/mockcloud")
		//os.RemoveAll("/tmp/localcache")
		config = localconfig
	} else {
		config = gcsIntconfig
	}
	return cloudstorage.NewStore(config)
}

type FileSource struct {
	ss         *schema.SourceSchema
	lastLoad   time.Time
	store      cloudstorage.Store
	tablenames []string
	tables     map[string]*schema.Table
}

func NewFileSource() schema.DataSource {
	m := FileSource{}
	return &m
}

func (m *FileSource) Setup(ss *schema.SourceSchema) error {
	m.ss = ss
	if m.lastLoad.Before(time.Now().Add(-schemaRefreshInterval)) {
		m.lastLoad = time.Now()
		u.Debugf("load schema")
		m.loadSchema()
	}
	return nil
}
func (m *FileSource) Open(source string) (schema.SourceConn, error) {
	u.Warnf("not implemented %q", source)
	return nil, schema.ErrNotImplemented
}
func (m *FileSource) Close() error {
	return nil
}
func (m *FileSource) Tables() []string {
	return m.tablenames
}

func (m *FileSource) loadSchema() {

	store, err := CreateStore()
	if err != nil {
		u.Errorf("Could not create cloudstore %v", err)
		return
	}

	u.Infof("%p  load schema %+v", m, m.ss.Conf)
	tableFolder := "tables/"
	if customFolder := m.ss.Conf.Settings.String("tables"); customFolder != "" {
		tableFolder = customFolder
		if !strings.HasSuffix(tableFolder, "/") {
			tableFolder = fmt.Sprintf("%s/", tableFolder)
		}
	}
	q := cloudstorage.NewQuery("tables/")
	//q.LimitMatch = ".csv"
	q.Sorted()
	objs, err := store.List(q)
	if err != nil {
		u.Errorf("could not open list err=%v", err)
		return
	}
	tables := make(map[string]struct{})
	tableList := make([]string, 0)
	for _, obj := range objs {
		fileName := obj.Name()
		fileName = strings.Replace(fileName, tableFolder, "", 1)
		parts := strings.Split(fileName, "/")
		if len(parts) > 1 {
			if _, exists := tables[parts[0]]; !exists {
				u.Infof("Nice, found new table: %q", parts[0])
				tables[parts[0]] = struct{}{}
				tableList = append(tableList, parts[0])
			}
		} else {
			u.Debugf("table not found %#v", obj)
		}
	}
	m.tablenames = tableList
}

func (m *FileSource) Table(tableName string) (*schema.Table, error) {

	/*
		tableName = strings.ToLower(tableName)
		if ds, ok := m.tables[tableName]; ok {
			return ds.Table(tableName)
		}
		err := m.loadTable(tableName)
		if err != nil {
			u.Errorf("could not load table %q  err=%v", tableName, err)
			return nil, err
		}
		ds, ok := m.tables[tableName]
		if !ok {
			return nil, schema.ErrNotFound
		}
		return ds.Table(tableName)
	*/
	return nil, schema.ErrNotImplemented
}

func (m *FileSource) loadTable(tableName string) error {
	/*

		tbl.SetColumns(csvSource.Columns())
		m.tables[tableName] = tbl

		// Now we are going to page through the Csv rows and Put into
		//  Static Data Source, ie copy into memory btree structure
		for {
			msg := csvSource.Next()
			if msg == nil {
				//u.Infof("table:%v  len=%v", tableName, tbl.Length())
				return nil
			}
			dm, ok := msg.Body().(*datasource.SqlDriverMessageMap)
			if !ok {
				return fmt.Errorf("Expected *datasource.SqlDriverMessageMap but got %T", msg.Body())
			}

			// We don't know the Key
			tbl.Put(nil, nil, dm.Values())
		}
	*/
	return nil
}
