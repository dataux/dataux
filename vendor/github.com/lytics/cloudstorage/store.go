package cloudstorage

import (
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/araddon/gou"
	"golang.org/x/net/context"
)

const (
	// StoreCacheFileExt = ".cache"
	StoreCacheFileExt = ".cache"
	// ContentTypeKey
	ContentTypeKey = "content_type"
	// MaxResults default number of objects to retrieve during a list-objects request,
	// if more objects exist, then they will need to be paged
	MaxResults = 3000
)

// AccessLevel is the level of permissions on files
type AccessLevel int

const (
	// ReadOnly File Permissions Levels
	ReadOnly  AccessLevel = 0
	ReadWrite AccessLevel = 1
)

var (
	// ErrObjectNotFound Error of not finding a file(object)
	ErrObjectNotFound = fmt.Errorf("object not found")
	// ErrObjectExists error trying to create an already existing file.
	ErrObjectExists = fmt.Errorf("object already exists in backing store (use store.Get)")
	// ErrNotImplemented this feature is not implemented for this store
	ErrNotImplemented = fmt.Errorf("Not implemented")
)

type (
	// StoreReader interface to define the Storage Interface abstracting
	// the GCS, S3, LocalFile, etc interfaces
	StoreReader interface {
		// Type is he Store Type [google, s3, azure, localfs, etc]
		Type() string
		// Client gets access to the underlying native Client for Google, S3, etc
		Client() interface{}
		// Get returns an object (file) from the cloud store. The object
		// isn't opened already, see Object.Open()
		// ObjectNotFound will be returned if the object is not found.
		Get(ctx context.Context, o string) (Object, error)
		// Objects returns an object Iterator to allow paging through object
		// which keeps track of page cursors.  Query defines the specific set
		// of filters to apply to request.
		Objects(ctx context.Context, q Query) (ObjectIterator, error)
		// List file/objects filter by given query.  This just wraps the object-iterator
		// returning full list of objects.
		List(ctx context.Context, q Query) (*ObjectsResponse, error)
		// Folders creates list of folders
		Folders(ctx context.Context, q Query) ([]string, error)
		// NewReader creates a new Reader to read the contents of the object.
		// ErrObjectNotFound will be returned if the object is not found.
		NewReader(o string) (io.ReadCloser, error)
		// NewReader with context (for cancelation, etc)
		NewReaderWithContext(ctx context.Context, o string) (io.ReadCloser, error)
		// String default descriptor.
		String() string
	}

	// StoreCopy Optional interface to fast path copy.  Many of the cloud providers
	// don't actually copy bytes.  Rather they allow a "pointer" that is a fast copy.
	StoreCopy interface {
		// Copy from object, to object
		Copy(ctx context.Context, src, dst Object) error
	}

	// StoreMove Optional interface to fast path move.  Many of the cloud providers
	// don't actually copy bytes.
	StoreMove interface {
		// Move from object location, to object location.
		Move(ctx context.Context, src, dst Object) error
	}

	// Store interface to define the Storage Interface abstracting
	// the GCS, S3, LocalFile interfaces
	Store interface {
		StoreReader

		// NewWriter returns a io.Writer that writes to a Cloud object
		// associated with this backing Store object.
		//
		// A new object will be created if an object with this name already exists.
		// Otherwise any previous object with the same name will be replaced.
		// The object will not be available (and any previous object will remain)
		// until Close has been called
		NewWriter(o string, metadata map[string]string) (io.WriteCloser, error)
		// NewWriter but with context.
		NewWriterWithContext(ctx context.Context, o string, metadata map[string]string) (io.WriteCloser, error)

		// NewObject creates a new empty object backed by the cloud store
		// This new object isn't' synced/created in the backing store
		// until the object is Closed/Sync'ed.
		NewObject(o string) (Object, error)

		// Delete removes the object from the cloud store.
		Delete(ctx context.Context, o string) error
	}

	// Object is a handle to a cloud stored file/object.  Calling Open will pull the remote file onto
	// your local filesystem for reading/writing.  Calling Sync/Close will push the local copy
	// backup to the cloud store.
	Object interface {
		// Name of object/file.
		Name() string
		// String is default descriptor.
		String() string
		// Updated timestamp.
		Updated() time.Time
		// MetaData is map of arbitrary name/value pairs about object.
		MetaData() map[string]string
		// SetMetaData allows you to set key/value pairs.
		SetMetaData(meta map[string]string)
		// StorageSource is the type of store.
		StorageSource() string
		// Open copies the remote file to a local cache and opens the cached version
		// for read/writing.  Calling Close/Sync will push the copy back to the
		// backing store.
		Open(readonly AccessLevel) (*os.File, error)
		// Release will remove the locally cached copy of the file.  You most call Close
		// before releasing.  Release will call os.Remove(local_copy_file) so opened
		// filehandles need to be closed.
		Release() error
		// Implement io.ReadWriteCloser Open most be called before using these
		// functions.
		Read(p []byte) (n int, err error)
		Write(p []byte) (n int, err error)
		Sync() error
		Close() error
		// File returns the cached/local copy of the file
		File() *os.File
		// Delete removes the object from the cloud store and local cache.
		Delete() error
	}

	// ObjectIterator interface to page through objects
	// See go doc for examples https://github.com/GoogleCloudPlatform/google-cloud-go/wiki/Iterator-Guidelines
	ObjectIterator interface {
		// Next gets next object, returns google.golang.org/api/iterator iterator.Done error.
		Next() (Object, error)
		// Close this down (and or context.Close)
		Close()
	}

	// ObjectsResponse for paged object apis.
	ObjectsResponse struct {
		Objects    Objects
		NextMarker string
	}
	// Objects are just a collection of Object(s).
	// Used as the results for store.List commands.
	Objects []Object

	// AuthMethod Is the source/location/type of auth token
	AuthMethod string

	// Config the cloud store config settings.
	Config struct {
		// Type is StoreType [gcs,localfs,s3,azure]
		Type string
		// AuthMethod the methods of authenticating store.  Ie, where/how to
		// find auth tokens.
		AuthMethod AuthMethod
		// Cloud Bucket Project
		Project string
		// Region is the cloud region
		Region string
		// Bucket is the "path" or named bucket in cloud
		Bucket string
		// the page size to use with api requests (default 1000)
		PageSize int
		// used by JWTKeySource
		JwtConf *JwtConf
		// JwtFile is the file-path to local auth-token file.
		JwtFile string `json:"jwtfile,omitempty"`
		// BaseUrl is the base-url path for customizing regions etc.  IE
		// AWS has different url paths per region on some situations.
		BaseUrl string `json:"baseurl,omitempty"`
		// Permissions scope
		Scope string `json:"scope,omitempty"`
		// LocalFS is filesystem path to use for the local files
		// for Type=localfs
		LocalFS string `json:"localfs,omitempty"`
		// The filesystem path to save locally cached files as they are
		// being read/written from cloud and need a staging area.
		TmpDir string `json:"tmpdir,omitempty"`
		// Settings are catch-all-bag to allow per-implementation over-rides
		Settings gou.JsonHelper `json:"settings,omitempty"`
		// LogPrefix Logging Prefix/Context message
		LogPrefix string
	}

	// JwtConf For use with google/google_jwttransporter.go
	// Which can be used by the google go sdk's.   This struct is based on the Google
	// Jwt files json for service accounts.
	JwtConf struct {
		// Unfortuneately we departed from the standard jwt service account field-naming
		// for reasons we forgot.  So, during load, we convert from bad->correct format.
		PrivateKeyDeprecated string `json:"private_keybase64,omitempty"`
		KeyTypeDeprecated    string `json:"keytype,omitempty"`

		// Jwt Service Account Fields
		ProjectID    string `json:"project_id,omitempty"`
		PrivateKeyID string `json:"private_key_id,omitempty"`
		PrivateKey   string `json:"private_key,omitempty"`
		ClientEmail  string `json:"client_email,omitempty"`
		ClientID     string `json:"client_id,omitempty"`
		Type         string `json:"type,omitempty"`
		// Scopes is list of what scope to use when the token is created.
		// for example https://github.com/google/google-api-go-client/blob/0d3983fb069cb6651353fc44c5cb604e263f2a93/storage/v1/storage-gen.go#L54
		Scopes []string `json:"scopes,omitempty"`
	}
)

// NewStore create new Store from Storage Config/Context.
func NewStore(conf *Config) (Store, error) {

	if conf.Type == "" {
		return nil, fmt.Errorf("Type is required on Config")
	}
	registryMu.RLock()
	st, ok := storeProviders[conf.Type]
	registryMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("config.Type=%q was not found", conf.Type)
	}

	if conf.PageSize == 0 {
		conf.PageSize = MaxResults
	}

	if conf.TmpDir == "" {
		conf.TmpDir = os.TempDir()
	}
	return st(conf)
}

// Copy source to destination.
func Copy(ctx context.Context, s Store, src, des Object) error {
	// for Providers that offer fast path, and use the backend copier
	if src.StorageSource() == des.StorageSource() {
		if cp, ok := s.(StoreCopy); ok {
			return cp.Copy(ctx, src, des)
		}
	}

	// Slow path, copy locally then up to des
	fout, err := des.Open(ReadWrite)
	if err != nil {
		return err
	}

	fin, err := src.Open(ReadOnly)
	if _, err = io.Copy(fout, fin); err != nil {
		return err
	}
	defer src.Close()

	return des.Close() //this will flush and sync the file.
}

// Move source object to destination.
func Move(ctx context.Context, s Store, src, des Object) error {
	// take the fast path, and use the store provided mover if available
	if src.StorageSource() == des.StorageSource() {
		if sm, ok := s.(StoreMove); ok {
			return sm.Move(ctx, src, des)
		}
	}

	// Slow path, copy locally then up to des
	fout, err := des.Open(ReadWrite)
	if err != nil {
		gou.Warnf("Move could not open destination %v", src.Name())
		return err
	}

	fin, err := src.Open(ReadOnly)
	if err != nil {
		gou.Warnf("Move could not open source %v err=%v", src.Name(), err)
	}
	if _, err = io.Copy(fout, fin); err != nil {
		return err
	}
	if err := src.Close(); err != nil {
		return err
	}
	if err := src.Delete(); err != nil {
		return err
	}

	return des.Close() //this will flush and sync the file.
}

func NewObjectsResponse() *ObjectsResponse {
	return &ObjectsResponse{
		Objects: make(Objects, 0),
	}
}
func (o Objects) Len() int           { return len(o) }
func (o Objects) Less(i, j int) bool { return o[i].Name() < o[j].Name() }
func (o Objects) Swap(i, j int)      { o[i], o[j] = o[j], o[i] }

// Validate that this is a valid jwt conf set of tokens
func (j *JwtConf) Validate() error {
	if j.PrivateKeyDeprecated != "" {
		j.PrivateKey = j.PrivateKeyDeprecated
		j.PrivateKeyDeprecated = ""
	}
	j.fixKey()
	if j.KeyTypeDeprecated != "" {
		j.Type = j.KeyTypeDeprecated
		j.KeyTypeDeprecated = ""
	}
	_, err := j.KeyBytes()
	if err != nil {
		return fmt.Errorf("Invalid JwtConf.PrivateKeyBase64  (error trying to decode base64 err: %v", err)
	}
	return nil
}
func (j *JwtConf) fixKey() {
	parts := strings.Split(j.PrivateKey, "\n")
	if len(parts) > 1 {
		for _, part := range parts {
			if strings.HasPrefix(part, "---") {
				continue
			}
			j.PrivateKey = part
			break
		}
	}
}
func (j *JwtConf) KeyBytes() ([]byte, error) {
	return base64.StdEncoding.DecodeString(j.PrivateKey)
}
