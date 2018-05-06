# Introduction
Cloudstorage is an library for working with Cloud Storage (Google, AWS, Azure) and SFTP, Local Files.
It provides a unified api for local files, sftp and Cloud files that aids testing and operating on multiple cloud storage.

[![Code Coverage](https://codecov.io/gh/lytics/cloudstorage/branch/master/graph/badge.svg)](https://codecov.io/gh/lytics/cloudstorage)
[![GoDoc](https://godoc.org/github.com/lytics/cloudstorage?status.svg)](http://godoc.org/github.com/lytics/cloudstorage)
[![Build Status](https://travis-ci.org/lytics/cloudstorage.svg?branch=master)](https://travis-ci.org/lytics/cloudstorage)
[![Go ReportCard](https://goreportcard.com/badge/lytics/cloudstorage)](https://goreportcard.com/report/lytics/cloudstorage)

**Features**
* Provide single unified api for multiple cloud (google, azure, aws) & local files.
* Cloud Upload/Download is unified in api so you don't have to download file to local, work with it, then upload.
* Buffer/Cache files from cloud local so speed of usage is very high.


### Similar/Related works
* https://github.com/graymeta/stow similar to this pkg, library for interacting with cloud services.  Less of the buffer/local cache.  Different clouds.
* https://github.com/ncw/rclone great cli sync tool, many connections (30+), well tested.  Designed as cli tool, config is less suited for use as library.


# Example usage:
Note: For these examples all errors are ignored, using the `_` for them.

##### Creating a Store object:
```go
// This is an example of a local storage object:  
// See(https://github.com/lytics/cloudstorage/blob/master/google/google_test.go) for a GCS example:
config := &cloudstorage.Config{
	Type: localfs.StoreType,
	AuthMethod:     localfs.AuthFileSystem,
	LocalFS:         "/tmp/mockcloud",
	TmpDir:          "/tmp/localcache",
}
store, _ := cloudstorage.NewStore(config)
```

##### Listing Objects:

See go Iterator pattern doc for api-design:
https://github.com/GoogleCloudPlatform/google-cloud-go/wiki/Iterator-Guidelines
```go
// From a store that has been created

// Create a query
q := cloudstorage.NewQuery("list-test/")
// Create an Iterator
iter, err := store.Objects(context.Background(), q)
if err != nil {
	// handle
}

for {
	o, err := iter.Next()
	if err == iterator.Done {
		break
	}
	log.Println("found object ", o.Name())
}
```

##### Writing an object :
```go
obj, _ := store.NewObject("prefix/test.csv")
// open for read and writing.  f is a filehandle to the local filesystem.
f, _ := obj.Open(cloudstorage.ReadWrite) 
w := bufio.NewWriter(f)
_, _ := w.WriteString("Year,Make,Model\n")
_, _ := w.WriteString("1997,Ford,E350\n")
w.Flush()

// Close sync's the local file to the remote store and removes the local tmp file.
obj.Close()
```


##### Reading an existing object:
```go
// Calling Get on an existing object will return a cloudstorage object or the cloudstorage.ErrObjectNotFound error.
obj2, _ := store.Get(context.Background(), "prefix/test.csv")
f2, _ := obj2.Open(cloudstorage.ReadOnly)
bytes, _ := ioutil.ReadAll(f2)
fmt.Println(string(bytes)) // should print the CSV file from the block above...
```

##### Transferring an existing object:
```go
var config = &storeutils.TransferConfig{
	Type:                  google.StoreType,
	AuthMethod:            google.AuthGCEDefaultOAuthToken
	ProjectID:             "my-project",
	DestBucket:            "my-destination-bucket",
	Src:                   storeutils.NewGcsSource("my-source-bucket"),
	IncludePrefxies:       []string{"these", "prefixes"},
}

transferer, _ := storeutils.NewTransferer(client)
resp, _ := transferer.NewTransfer(config)

```

See [testsuite.go](https://github.com/lytics/cloudstorage/blob/master/testutils/testutils.go) for more examples

## Testing

Due to the way integration tests act against a cloud bucket and objects; run tests without parallelization. 

```
cd $GOPATH/src/github.com/lytics/cloudstorage
go test -p 1 ./...
```

