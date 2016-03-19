package files

import (
	"strings"
	"sync"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/schema"
)

var (
	// the global file-scanners registry mutex
	registryMu sync.Mutex
	registry   = newScannerRegistry()
)

func init() {
	RegisterFileScanner("csv", csvMaker)
}

// a factory to create Scanners for a speciffic format type such as csv, json
type FileScannerMaker func(*FileInfo) (schema.Scanner, error)

// Register a file scanner maker available by the provided @scannerType
func RegisterFileScanner(scannerType string, scanner FileScannerMaker) {
	if scanner == nil {
		panic("File scanners must not be nil")
	}
	scannerType = strings.ToLower(scannerType)
	u.Debugf("global file-scanner register: %v %T scanner:%p", scannerType, scanner, scanner)
	registryMu.Lock()
	defer registryMu.Unlock()
	if _, dupe := registry.scanners[scannerType]; dupe {
		panic("Register called twice for scanner type " + scannerType)
	}
	registry.scanners[scannerType] = scanner
}

// Our internal map of different types of datasources that are registered
// for our runtime system to use
type scannerRegistry struct {
	// Map of scanner name, to maker
	scanners map[string]FileScannerMaker
}

func newScannerRegistry() *scannerRegistry {
	return &scannerRegistry{
		scanners: make(map[string]FileScannerMaker),
	}
}

func scannerGet(scannerType string) (FileScannerMaker, bool) {
	registryMu.Lock()
	defer registryMu.Unlock()
	scannerType = strings.ToLower(scannerType)
	scanner, ok := registry.scanners[scannerType]
	return scanner, ok
}

func csvMaker(f *FileInfo) (schema.Scanner, error) {
	csv, err := datasource.NewCsvSource(f.Table, 0, f.F, f.Exit)
	if err != nil {
		u.Errorf("Could not open file for csv reading %v", err)
		return nil, err
	}
	return csv, nil
}
