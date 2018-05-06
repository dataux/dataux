/*
Package cloudstorage is an interface to make Local, Google, s3 file storage
share a common interface to aid testing local as well as
running in the cloud.

The primary goal is to create a Store which is a common interface
over each of the (google, s3, local-file-system, azure) etc file storage
systems.   Then the methods (Query, filter, get, put) are common, as are
the Files (Objects) themselves.  Writing code that supports multiple
backends is now simple.


Creating and iterating files

In this example we are going to create a local-filesystem
store.

	// This is an example of a local-storage (local filesystem) provider:
	config := &cloudstorage.Config{
		Type: localfs.StoreType,
		TokenSource:     localfs.AuthFileSystem,
		LocalFS:         "/tmp/mockcloud",
		TmpDir:          "/tmp/localcache",
	}
	store, _ := cloudstorage.NewStore(config)

	// Create a query to define the search path
	q := cloudstorage.NewQuery("list-test/")

	// Create an Iterator to list files
	iter := store.Objects(context.Background(), q)
	for {
		o, err := iter.Next()
		if err == iterator.Done {
			break
		}
		log.Println("found object %v", o.Name())
	}
*/
package cloudstorage
