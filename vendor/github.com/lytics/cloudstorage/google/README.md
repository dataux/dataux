google cloud storage store
--------------------------
Cloudstorage abstraction package for gcs.



```sh
# the CloudStorage GCS JWT key is an env with full jwt token json encoded.
export CS_GCS_JWTKEY="{\"project_id\": \"lio-testing\", \"private_key_id\": \"

```


## Example
```go

// example with  CS_GCS_JWTKEY env var
conf := &cloudstorage.Config{
	Type:       google.StoreType,
	AuthMethod: google.AuthJWTKeySource,
	Project:    "my-google-project",
	Bucket:     "integration-tests-nl",
	TmpDir:     "/tmp/localcache/google",
}

// OR read from machine oauth locations
conf := &cloudstorage.Config{
	Type:       google.StoreType,
	AuthMethod: google.AuthGCEDefaultOAuthToken,
	Project:    "my-google-project",
	Bucket:     "integration-tests-nl",
	TmpDir:     "/tmp/localcache/google",
}

// OR metadata api if on google cloud
conf := &cloudstorage.Config{
	Type:       google.StoreType,
	AuthMethod: google.AuthGCEMetaKeySource,
	Project:    "my-google-project",
	Bucket:     "integration-tests-nl",
	TmpDir:     "/tmp/localcache/google",
}

// create store
store, err := cloudstorage.NewStore(conf)
if err != nil {
    return err
}


```