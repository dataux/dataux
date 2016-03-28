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
	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/plan"
	"github.com/araddon/qlbridge/schema"
)

var (
	// ensure we implement interfaces
	_ schema.Source = (*FileSource)(nil)

	// Our file-pager wraps our file-scanners to move onto next file
	_ PartitionedFileReader = (*FilePager)(nil)
	_ schema.ConnScanner    = (*FilePager)(nil)
	_ exec.ExecutorSource   = (*FilePager)(nil)
	//_ exec.RequiresContext  = (*FilePager)(nil)

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

	// FileStoreLoader defines the interface for loading files
	FileStoreLoader func(ss *schema.SchemaSource) (cloudstorage.Store, error)
)

const (
	// SourceType is the registered Source name in the qlbridge source registry
	SourceType = "cloudstore"
)

func init() {
	// We need to register our DataSource provider here
	datasource.Register(SourceType, NewFileSource())
	FileStoreLoader = createConfStore
}

// PartitionedFileReader defines a file source that can page through files
type PartitionedFileReader interface {
	// NextFile returns io.EOF on last file
	NextFile() (*FileReader, error)
}

// FileSource DataSource for reading files, and scanning them allowing
//  the contents to be treated as a database, like doing a full
//  table scan in mysql.  But, you can partition across files.
//
// - readers:      s3, gcs, local-fs
// - tablesource:  translate lists of files into tables.  Normally we would have
//                 multiple files per table (ie partitioned, per-day, etc)
// - scanners:     responsible for file-specific
//
type FileSource struct {
	ss             *schema.SchemaSource
	lastLoad       time.Time
	store          cloudstorage.Store
	fh             FileHandler
	tablenames     []string
	tables         map[string]*schema.Table
	files          map[string][]*FileInfo
	path           string
	tablePerFolder bool
	fileType       string // csv, json, proto, customname
	Partitioner    string // random, ??  (date, keyed?)
}

// FileInfo Struct of file info
type FileInfo struct {
	Name      string // Name, Path of file
	Table     string // Table name this file participates in
	FileType  string // csv, json, etc
	Partition int    // which partition
}

// FileReader file info and access to file to supply to ScannerMakers
type FileReader struct {
	*FileInfo
	F    io.Reader // Actual file reader
	Exit chan bool // exit channel to shutdown reader
}

// NewFileSource provides single FileSource
func NewFileSource() schema.Source {
	m := FileSource{
		tables:     make(map[string]*schema.Table),
		files:      make(map[string][]*FileInfo),
		tablenames: make([]string, 0),
	}
	return &m
}

// Setup the filesource with schema info
func (m *FileSource) Setup(ss *schema.SchemaSource) error {
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

// Open a connection to given table, part of Source interface
func (m *FileSource) Open(tableName string) (schema.Conn, error) {
	pg, err := m.createPager(tableName, 0)
	if err != nil {
		u.Errorf("could not get pager: %v", err)
		return nil, err
	}
	// _, err = pg.NextScanner()
	// if err != nil {
	// 	u.Warnf("why error? %v", err)
	// 	return nil, err
	// }
	return pg, nil
}

// Close this Conn
func (m *FileSource) Close() error { return nil }

// Tables for this file-source
func (m *FileSource) Tables() []string { return m.tablenames }
func (m *FileSource) init() error {
	if m.store == nil {
		store, err := FileStoreLoader(m.ss)
		if err != nil {
			u.Errorf("Could not create cloudstore %v", err)
			return err
		}
		m.store = store

		// filesTable := fmt.Sprintf("%s_files", m.ss.Name)
		// m.tablenames = append(m.tablenames, filesTable)

		conf := m.ss.Conf.Settings
		if tablePath := conf.String("path"); tablePath != "" {
			m.path = tablePath
		}
		if fileType := conf.String("format"); fileType != "" {
			m.fileType = fileType
		}
		if partitioner := conf.String("partitioner"); partitioner != "" {
			m.Partitioner = partitioner
		}

		// TODO:   if no m.fileType inspect file name?
		fileHandler, exists := scannerGet(m.fileType)
		if !exists || fileHandler == nil {
			return fmt.Errorf("Could not find scanner for filetype %q", m.fileType)
		}
		m.fh = fileHandler
	}
	return nil
}

func fileInterpret(path string, obj cloudstorage.Object) *FileInfo {
	fileName := obj.Name()
	//u.Debugf("file %s", fileName)
	fileName = strings.Replace(fileName, path, "", 1)
	// Look for Folders
	parts := strings.Split(fileName, "/")
	if len(parts) > 1 {
		return &FileInfo{Table: parts[0], Name: obj.Name()}
	}
	parts = strings.Split(fileName, ".")
	if len(parts) > 1 {
		tableName := strings.ToLower(parts[0])
		return &FileInfo{Table: tableName, Name: obj.Name()}
	}
	u.Errorf("table not readable from filename %q  %#v", fileName, obj)
	return nil
}

func (m *FileSource) loadSchema() {

	//u.Infof("%p  load schema %#v", m, m.ss.Conf)

	q := cloudstorage.Query{Prefix: m.path}
	q.Sorted() // We need to sort this by reverse to go back to front?
	objs, err := m.store.List(q)
	if err != nil {
		u.Errorf("could not open list err=%v", err)
		return
	}
	nextPartId := 0

	for _, obj := range objs {
		fi := m.fh.File(m.path, obj)
		if fi == nil {
			continue
		}
		if fi.Name != "" {
			if _, tableExists := m.files[fi.Table]; !tableExists {
				u.Debugf("%p found new table: %q", m, fi.Table)
				m.files[fi.Table] = make([]*FileInfo, 0)
				m.tablenames = append(m.tablenames, fi.Table)
			}

			m.files[fi.Table] = append(m.files[fi.Table], fi)
		}
		if fi.Partition == 0 && m.ss.Conf.PartitionCt > 0 {
			// assign a partition
			fi.Partition = nextPartId
			//u.Debugf("%d found file part:%d  %s", len(m.files[fi.Table]), fi.Partition, fi.Name)
			nextPartId++
			if nextPartId >= m.ss.Conf.PartitionCt {
				nextPartId = 0
			}
		}
	}
}

// Table satisfys Source Schema interface to get table schema for given table
func (m *FileSource) Table(tableName string) (*schema.Table, error) {

	var err error
	//u.Debugf("Table(%q)", tableName)
	t, ok := m.tables[tableName]
	if ok {
		return t, nil
	}

	// Its possible that the file handle implements schema handling
	if schemaSource, hasSchema := m.fh.(schema.SourceTableSchema); hasSchema {

		t, err = schemaSource.Table(tableName)
		if err != nil {
			u.Errorf("could not get %T P:%p table %q %v", schemaSource, schemaSource, tableName, err)
			return nil, err
		}

	} else {

		t, err = m.buildTable(tableName)
		if err != nil {
			return nil, err
		}

	}

	if t == nil {
		u.Warnf("Nil Table, should not be possible? %q", tableName)
		return nil, fmt.Errorf("Missing table for %q", tableName)
	}

	m.tables[tableName] = t
	return t, nil
}

func (m *FileSource) buildTable(tableName string) (*schema.Table, error) {

	// Since we don't have a table schema, lets create one via introspection
	u.Debugf("We are introspecting table %q for schema, provide a schema if you want better schema", tableName)
	pager, err := m.createPager(tableName, 0)
	if err != nil {
		u.Errorf("could not find scanner for table %q table err:%v", tableName, err)
		return nil, err
	}

	scanner, err := pager.NextScanner()
	if err != nil {
		u.Errorf("what, no scanner? %v", err)
		return nil, err
	}

	colScanner, hasColumns := scanner.(schema.ConnColumns)
	if !hasColumns {
		return nil, fmt.Errorf("Must have Columns to Introspect Tables")
	}

	t := schema.NewTable(tableName, nil)
	t.SetColumns(colScanner.Columns())

	// we are going to look at ~10 rows to create schema for it
	if err = datasource.IntrospectTable(t, scanner); err != nil {
		u.Errorf("Could not introspect schema %v", err)
		return nil, err
	}

	return t, nil
}

func (m *FileSource) createPager(tableName string, partition int) (*FilePager, error) {

	// Read the object from cloud storage
	files := m.files[tableName]
	if len(files) == 0 {
		return nil, schema.ErrNotFound
	}

	pg := &FilePager{
		files: files,
		fs:    m,
		table: tableName,
	}
	return pg, nil
}

// FilePager acts like a Conn, wrapping underlying FileSource
// and paging through looking at partitions
type FilePager struct {
	cursor    int
	rowct     int64
	table     string
	fs        *FileSource
	files     []*FileInfo
	partition *schema.Partition
	partid    int
	tbl       *schema.Table
	p         *plan.Source
	schema.ConnScanner
}

// func (m *FilePager) SetContext(ctx *plan.Context) {
// 	m.ctx = ctx
// }

// WalkExecSource Provide ability to implement a source plan for execution
func (m *FilePager) WalkExecSource(p *plan.Source) (exec.Task, error) {

	if m.p == nil {
		m.p = p
		//u.Debugf("%p custom? %v", m, p.Custom)
		if partitionId, ok := p.Custom.IntSafe("partition"); ok {
			m.partid = partitionId
		}
	} else {
		u.Warnf("not nil?  custom? %v", p.Custom)
	}

	//u.Debugf("WalkExecSource():  %T  %#v", p, p)
	return exec.NewSource(p.Context(), p)
}

// Columns part of Conn interface for providing columns for this table/conn
func (m *FilePager) Columns() []string {
	if m.tbl == nil {
		t, err := m.fs.Table(m.table)
		if err != nil {
			u.Warnf("error getting table? %v", err)
			return nil
		}
		m.tbl = t
	}
	return m.tbl.Columns()
}

// NextScanner provides the next scanner assuming that each scanner
// representas different file, and multiple files for single source
func (m *FilePager) NextScanner() (schema.ConnScanner, error) {

	fr, err := m.NextFile()
	if err == io.EOF {
		return nil, err
	}

	// if m.p == nil {
	// 	u.Debugf("%p MASTER next file partid:%d %v", m, m.partid, fr.Name)
	// 	u.WarnT(12)
	// } else {
	// 	u.Debugf("%p ACTOR next file partid:%d  custom:%v %v", m, m.partid, m.p.Custom, fr.Name)
	// }
	//u.Debugf("%p next file partid:%d  custom:%v %v", m, m.partid, m.p.Custom, fr.Name)
	scanner, err := m.fs.fh.Scanner(m.fs.store, fr)
	if err != nil {
		u.Errorf("Could not open file scanner %v err=%v", m.fs.fileType, err)
		return nil, err
	}
	m.ConnScanner = scanner
	return scanner, err
}

// NextFile gets next file
func (m *FilePager) NextFile() (*FileReader, error) {

	var fi *FileInfo
	for {
		if m.cursor >= len(m.files) {
			return nil, io.EOF
		}
		fi = m.files[m.cursor]
		m.cursor++
		if fi.Partition == m.partid {
			break
		}
	}

	//u.Debugf("%p opening: partition:%v desiredpart:%v file: %q ", m, fi.Partition, m.partid, fi.Name)
	obj, err := m.fs.store.Get(fi.Name)
	if err != nil {
		u.Errorf("could not read %q table %v", err)
		return nil, err
	}

	f, err := obj.Open(cloudstorage.ReadOnly)
	if err != nil {
		u.Errorf("could not read %q table %v", m.table, err)
		return nil, err
	}
	//u.Infof("found file: %s   %p", obj.Name(), f)

	fr := &FileReader{
		F:        f,
		Exit:     make(chan bool),
		FileInfo: fi,
	}
	return fr, nil
}

// Next iterator for next message, wraps the file Scanner, Next file abstractions
func (m *FilePager) Next() schema.Message {
	if m.ConnScanner == nil {
		m.NextScanner()
	}
	for {
		msg := m.ConnScanner.Next()
		if msg == nil {
			_, err := m.NextScanner()
			if err != nil && err == io.EOF {
				return nil
			} else if err != nil {
				u.Errorf("unexpected end of scan %v", err)
				return nil
			}
			// now that we have a new scanner, lets try again
			if m.ConnScanner != nil {
				u.Debugf("next page")
				msg = m.ConnScanner.Next()
			}
		}

		//u.Infof("msg: %#v", msg.Body())

		m.rowct++

		return msg
	}
}

// Close this connection/pager
func (m *FilePager) Close() error {
	return nil
}

func createConfStore(ss *schema.SchemaSource) (cloudstorage.Store, error) {

	if ss == nil || ss.Conf == nil {
		return nil, fmt.Errorf("No config info for files source")
	}
	u.Infof("json conf:\n%s", ss.Conf.Settings.PrettyJson())
	cloudstorage.LogConstructor = func(prefix string) logging.Logger {
		return logging.NewStdLogger(true, logging.DEBUG, prefix)
	}

	var config *cloudstorage.CloudStoreContext
	conf := ss.Conf.Settings
	storeType := ss.Conf.Settings.String("type")
	switch storeType {
	case "gcs":
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

		c := cloudstorage.CloudStoreContext{
			LogggingContext: "localfiles",
			TokenSource:     cloudstorage.LocalFileSource,
			LocalFS:         "/tmp/mockcloud",
			TmpDir:          "/tmp/localcache",
		}
		if path := conf.String("path"); path != "" {
			c.LocalFS = path
		}
		//os.RemoveAll("/tmp/localcache")

		config = &c
	default:
		return nil, fmt.Errorf("Unrecognized filestore type %q expected [gcs,localfs]", storeType)
	}
	u.Debugf("creating cloudstore from %#v", config)
	return cloudstorage.NewStore(config)
}
