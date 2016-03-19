package files

import (
	"fmt"
	"io"
	"strings"
	"time"

	u "github.com/araddon/gou"
	"github.com/lytics/cloudstorage"
	"github.com/lytics/cloudstorage/logging"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/schema"
)

var (
	// ensure we implement interfaces
	_ schema.DataSource = (*FileSource)(nil)

	schemaRefreshInterval = time.Minute * 5

	_ = u.EMPTY

	// TODO:   move to test files
	localFilesConfig = cloudstorage.CloudStoreContext{
		LogggingContext: "unittest",
		TokenSource:     cloudstorage.LocalFileSource,
		LocalFS:         "/tmp/mockcloud",
		TmpDir:          "/tmp/localcache",
	}

	// TODO:   complete manufacture this from config
	gcsConfig = cloudstorage.CloudStoreContext{
		LogggingContext: "dataux",
		TokenSource:     cloudstorage.GCEDefaultOAuthToken,
		Project:         "lytics-dev",
		Bucket:          "lytics-dataux-tests",
		TmpDir:          "/tmp/localcache",
	}
)

const (
	SourceType = "cloudstore"
)

func init() {
	// We need to register our DataSource provider here
	datasource.Register(SourceType, NewFileSource())
}

// File DataSource for reading files, and scanning them allowing
//  the rows to be treated as a scannable dataset, like doing a full
//  scan in mysql.
//
// - readers:      s3, gcs, local-fs
// - tablesource:  translate lists of files into tables.  Normally we would have
//                 multiple files per table (ie partitioned, per-day, etc)
// - scanners:     responsible for file-specific
//
type FileSource struct {
	ss             *schema.SourceSchema
	lastLoad       time.Time
	store          cloudstorage.Store
	tablenames     []string
	tables         map[string]*schema.Table
	files          map[string][]string
	path           string
	tablePerFolder bool
	fileType       string // csv, json, proto, customname
}

// Struct of file info to supply to ScannerMakers
type FileInfo struct {
	Name      string    // Name, Path of file
	Table     string    // Table name this file participates in
	FileType  string    // csv, json, etc
	Partition int       // which partition
	F         io.Reader // Actual file reader
	Exit      chan bool // exit channel to shutdown reader
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

	u.Debugf("files open %q", tableName)
	files := m.files[tableName]
	//u.Debugf("filenames: ct=%d  %v", len(files), files)
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
		if fileType := conf.String("format"); fileType != "" {
			m.fileType = fileType
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
	fi := &FileInfo{
		F:     f,
		Exit:  exit,
		Table: tableName,
	}

	// TODO:   if no m.fileType inspect file name?
	scannerMaker, exists := scannerGet(m.fileType)
	if !exists || scannerMaker == nil {
		return nil, fmt.Errorf("Could not find scanner for filetype %q", m.fileType)
	}
	scanner, err := scannerMaker(fi)
	if err != nil {
		u.Errorf("Could not open file scanner %v err=%v", m.fileType, err)
		return nil, err
	}

	t := schema.NewTable(tableName, nil)
	t.SetColumns(scanner.Columns())

	iter := scanner.CreateIterator(nil)

	// we are going to look at ~10 rows to create schema for it
	if err = datasource.IntrospectTable(t, iter); err != nil {
		u.Errorf("Could not introspect schema %v", err)
		return nil, err
	}

	m.tables[tableName] = t
	return t, nil
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
