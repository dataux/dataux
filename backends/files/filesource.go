package files

import (
	"fmt"
	"strings"
	"time"

	u "github.com/araddon/gou"
	"github.com/lytics/cloudstorage"
	"github.com/lytics/cloudstorage/logging"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/schema"
)

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

var localFilesConfig = cloudstorage.CloudStoreContext{
	LogggingContext: "unittest",
	TokenSource:     cloudstorage.LocalFileSource,
	LocalFS:         "/tmp/mockcloud",
	TmpDir:          "/tmp/localcache",
}

var gcsConfig = cloudstorage.CloudStoreContext{
	LogggingContext: "dataux",
	TokenSource:     cloudstorage.GCEDefaultOAuthToken,
	Project:         "lytics-dev",
	Bucket:          "lytics-dataux-tests",
	TmpDir:          "/tmp/localcache",
}

func createConfStore(ss *schema.SourceSchema) (cloudstorage.Store, error) {

	if ss == nil || ss.Conf == nil {
		return nil, fmt.Errorf("No config info for files source")
	}
	u.Infof("json conf:\n%s", ss.Conf.Settings.PrettyJson())
	cloudstorage.LogConstructor = func(prefix string) logging.Logger {
		return logging.NewStdLogger(true, logging.DEBUG, prefix)
	}

	var config *cloudstorage.CloudStoreContext
	conf := ss.Conf.Settings
	switch ss.Conf.Settings.String("type") {
	case "gcs", "":
		c := gcsConfig
		if proj := conf.String("project"); proj != "" {
			c.Project = proj
		}
		if bkt := conf.String("bucket"); bkt != "" {
			c.Bucket = bkt
		}
		if jwt := conf.String("jwt"); jwt != "" {
			c.JwtFile = jwt
		}
		config = &c
	case "localfs":
		//os.RemoveAll("/tmp/mockcloud")
		//os.RemoveAll("/tmp/localcache")
		c := localFilesConfig
		config = &c
	}
	return cloudstorage.NewStore(config)
}

type FileSource struct {
	ss             *schema.SourceSchema
	lastLoad       time.Time
	store          cloudstorage.Store
	tablenames     []string
	tables         map[string]*schema.Table
	files          map[string][]string
	path           string
	tablePerFolder bool
}

func NewFileSource() schema.DataSource {
	m := FileSource{tables: make(map[string]*schema.Table)}
	return &m
}

func (m *FileSource) Setup(ss *schema.SourceSchema) error {
	m.ss = ss
	if err := m.init(); err != nil {
		return err
	}
	if m.lastLoad.Before(time.Now().Add(-schemaRefreshInterval)) {
		m.lastLoad = time.Now()
		m.loadSchema()
	}
	return nil
}

func (m *FileSource) Open(tableName string) (schema.SourceConn, error) {
	u.Warnf("open %q", tableName)

	files := m.files[tableName]
	u.Debugf("filenames: ct=%d  %v", len(files), files)
	if len(files) == 0 {
		return nil, fmt.Errorf("Not files found for %q", tableName)
	}
	//Read the object back out of the cloud storage.
	obj, err := m.store.Get(files[0])
	if err != nil {
		u.Errorf("could not read %q table %v", tableName, err)
		return nil, err
	}

	f, err := obj.Open(cloudstorage.ReadOnly)
	if err != nil {
		u.Errorf("could not read %q table %v", tableName, err)
		return nil, err
	}
	u.Infof("found file/table: %s   %p", obj.Name(), f)

	exit := make(chan bool)
	csv, err := datasource.NewCsvSource(tableName, 0, f, exit)
	if err != nil {
		u.Errorf("Could not open file for csv reading %v", err)
		return nil, err
	}

	return csv, nil
}
func (m *FileSource) Close() error     { return nil }
func (m *FileSource) Tables() []string { return m.tablenames }
func (m *FileSource) init() error {
	if m.store == nil {
		store, err := createConfStore(m.ss)
		if err != nil {
			u.Errorf("Could not create cloudstore %v", err)
			return err
		}
		m.store = store

		conf := m.ss.Conf.Settings
		if tablePath := conf.String("path"); tablePath != "" {
			m.path = tablePath
		}
	}
	return nil
}

func (m *FileSource) loadSchema() {

	u.Infof("%p  load schema %+v", m, m.ss.Conf)

	q := cloudstorage.NewQuery(m.path)
	//q.LimitMatch = ".csv"
	//q.Delimiter = "/"
	q.Sorted()
	objs, err := m.store.List(q)
	if err != nil {
		u.Errorf("could not open list err=%v", err)
		return
	}
	tables := make(map[string]struct{})
	m.files = make(map[string][]string)
	tableList := make([]string, 0)
	//u.Debugf("found %d files", len(objs))
	for _, obj := range objs {
		fileName := obj.Name()
		//u.Debugf("file %s", fileName)
		fileName = strings.Replace(fileName, m.path, "", 1)
		parts := strings.Split(fileName, "/")
		if len(parts) > 1 {
			if _, exists := tables[parts[0]]; !exists {
				//u.Infof("Nice, found new table: %q", parts[0])
				tables[parts[0]] = struct{}{}
				tableList = append(tableList, parts[0])
			}
			m.files[parts[0]] = append(m.files[parts[0]], obj.Name())
		} else {
			parts = strings.Split(fileName, ".")
			if len(parts) > 1 {
				tableName := strings.ToLower(parts[0])
				if _, exists := tables[tableName]; !exists {
					//u.Infof("Nice, found new table: %q", tableName)
					tables[parts[0]] = struct{}{}
					tableList = append(tableList, tableName)
				}
				m.files[tableName] = append(m.files[tableName], obj.Name())
			} else {
				u.Errorf("table not readable from filename %#v", obj)
			}
		}
	}
	m.tablenames = tableList
	u.Debugf("tables:  %v", tableList)
	//u.Debugf("files: %v", m.files)
}

func (m *FileSource) Table(tableName string) (*schema.Table, error) {

	u.Infof("Table(%q)", tableName)
	//tableName = strings.ToLower(tableName)
	if t, ok := m.tables[tableName]; ok {
		return t, nil
	}

	//Read the object back out of the cloud storage and introspect
	files := m.files[tableName]
	if len(files) == 0 {
		return nil, schema.ErrNotFound
	}
	fileName := files[0]
	// fileName = "tables/article/article1.csv"
	u.Debugf("opening: %q", fileName)
	obj, err := m.store.Get(fileName)
	if err != nil {
		u.Errorf("could not read %q table %v", tableName, err)
		return nil, err
	}

	f, err := obj.Open(cloudstorage.ReadOnly)
	if err != nil {
		u.Errorf("could not read %q table %v", tableName, err)
		return nil, err
	}
	u.Infof("found file/table: %s   %p", obj.Name(), f)

	exit := make(chan bool)
	csv, err := datasource.NewCsvSource(tableName, 0, f, exit)
	if err != nil {
		u.Errorf("Could not open file for csv reading %v", err)
		return nil, err
	}

	t := schema.NewTable(tableName, nil)
	t.SetColumns(csv.Columns())

	iter := csv.CreateIterator(nil)

	if err = datasource.IntrospectTable(t, iter); err != nil {
		u.Errorf("Could not introspect schema %v", err)
		return nil, err
	}

	m.tables[tableName] = t
	return t, nil
}
