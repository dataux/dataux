grpc
====

When new versions of gRPC are released, the wire.proto can be
processed by running the commands below from the base of the
git repository.

```
# Make sure your protoc is version 3.1+
protoc --version

# Go get and generate.
go get google.golang.org/grpc
go get github.com/golang/protobuf/proto
go get github.com/golang/protobuf/protoc-gen-go
protoc -I grid.v3 grid.v3/wire.proto --go_out=plugins=grpc:grid.v3
```
