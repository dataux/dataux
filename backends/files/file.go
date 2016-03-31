package files

import (
	"database/sql/driver"
	"io"

	"github.com/lytics/cloudstorage"
)

var (
	// FileColumns are the default file-columns
	FileColumns = []string{"file", "table", "path", "size", "partition", "updated", "deleted", "filetype"}
)

// FileInfo Struct of file info
type FileInfo struct {
	obj       cloudstorage.Object
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

// Values as as slice
func (m *FileInfo) Values() []driver.Value {
	return []driver.Value{
		m.Name,
		m.Table,
		"",
		0,
		m.Partition,
		m.obj.Updated(),
		false,
		m.FileType,
	}
}
