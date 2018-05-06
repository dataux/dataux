package google

import (
	"github.com/lytics/cloudstorage"
	"google.golang.org/api/storage/v1"
)

// APIStore a google api store
type APIStore struct {
	service *storage.Service
	project string
}

// NewAPIStore create api store.
func NewAPIStore(conf *cloudstorage.Config) (*APIStore, error) {
	googleClient, err := NewGoogleClient(conf)
	if err != nil {
		return nil, err
	}
	service, err := storage.New(googleClient.Client())
	if err != nil {
		return nil, err
	}
	return &APIStore{service: service, project: conf.Project}, nil
}

// BucketExists checks for the bucket name
func (c *APIStore) BucketExists(name string) bool {
	b, err := c.service.Buckets.Get(name).Do()
	if err != nil {
		return false
	}

	return b.Id != ""
}

// CreateBucket creates a new bucket in GCS
func (c *APIStore) CreateBucket(name string) error {
	bucket := &storage.Bucket{Name: name}
	_, err := c.service.Buckets.Insert(c.project, bucket).Do()
	return err
}

// AddOwner adds entity as a owner of the object
func (c *APIStore) AddOwner(bucket, object, entity string) error {
	ac := &storage.ObjectAccessControl{Entity: entity, Role: "OWNER"}
	_, err := c.service.ObjectAccessControls.Insert(bucket, object, ac).Do()
	return err
}

// AddReader adds enitty as a reader of the object
func (c *APIStore) AddReader(bucket, object, entity string) error {
	ac := &storage.ObjectAccessControl{Entity: entity, Role: "READER"}
	_, err := c.service.ObjectAccessControls.Insert(bucket, object, ac).Do()
	return err
}
