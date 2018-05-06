


- [ ] Working some util for prefetching files as a background job (i.e. a cointinus stream of files).  The prefetched files would be handed a list of files (or query) and then it will begin prefetching the files as a consumer processes from the other end of the pipe(channel).  Backpressuring if the consumer slows down.  When downloading hundreds in a download->process->close loop, the loop will experince pauses as the fetching of files from the cloudstore take time.  Prefetching elements removes most of the pausing ,espiecially if the files take a long time to process.

- [ ] Add support for stream reading/writing without a local tmp file.  Like https://golang.org/pkg/bufio/ 

- [ ] Research supporting multipart uploads to mutiple files, then using the https://cloud.google.com/storage/docs/json_api/v1/objects/compose api to concatenate them into one. 
   -  https://cloud.google.com/storage/docs/composite-objects

- [ ] Create a store to store functions.  
   - [ ] Rsync: `store.Rsync(s *Store)`, Rsync could be used to sync all files between GCS and S3.
