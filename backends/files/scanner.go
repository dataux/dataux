package files

import (
	"strings"
	"sync"

	u "github.com/araddon/gou"
	"github.com/lytics/cloudstorage"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/schema"
)

var (
	// the global file-scanners registry mutex
	registryMu sync.Mutex
	registry   = newScannerRegistry()

	_ FileHandler = (*csvFiles)(nil)
)

func init() {
	RegisterFileScanner("csv", &csvFiles{})
}

// a factory to create Scanners for a speciffic format type such as csv, json
type FileHandler interface {
	File(path string, obj cloudstorage.Object) (table string, isFile bool)
	Scanner(store cloudstorage.Store, f *FileInfo) (schema.Scanner, error)
}

// Register a file scanner maker available by the provided @scannerType
func RegisterFileScanner(scannerType string, fh FileHandler) {
	if fh == nil {
		panic("File scanners must not be nil")
	}
	scannerType = strings.ToLower(scannerType)
	u.Debugf("global FileHandler register: %v %T FileHandler:%p", scannerType, fh, fh)
	registryMu.Lock()
	defer registryMu.Unlock()
	if _, dupe := registry.scanners[scannerType]; dupe {
		panic("Register called twice for FileHandler type " + scannerType)
	}
	registry.scanners[scannerType] = fh
}

// Our internal map of different types of datasources that are registered
// for our runtime system to use
type scannerRegistry struct {
	// Map of scanner name, to maker
	scanners map[string]FileHandler
}

func newScannerRegistry() *scannerRegistry {
	return &scannerRegistry{
		scanners: make(map[string]FileHandler),
	}
}

func scannerGet(scannerType string) (FileHandler, bool) {
	registryMu.Lock()
	defer registryMu.Unlock()
	scannerType = strings.ToLower(scannerType)
	scanner, ok := registry.scanners[scannerType]
	return scanner, ok
}

type csvFiles struct {
}

func (m *csvFiles) File(path string, obj cloudstorage.Object) (string, bool) {
	return FileInterpret(path, obj)
}
func (m *csvFiles) Scanner(store cloudstorage.Store, f *FileInfo) (schema.Scanner, error) {
	csv, err := datasource.NewCsvSource(f.Table, 0, f.F, f.Exit)
	if err != nil {
		u.Errorf("Could not open file for csv reading %v", err)
		return nil, err
	}
	return csv, nil
}
