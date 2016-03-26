#! /bin/sh



cd $GOPATH/src/github.com/araddon/dateparse && git checkout master && git pull
cd $GOPATH/src/github.com/araddon/gou && git checkout master && git pull
#cd $GOPATH/src/github.com/araddon/qlbridge && git checkout master && git pull
cd $GOPATH/src/github.com/bitly/go-hostpool && git checkout master && git pull
cd $GOPATH/src/github.com/bmizerany/assert && git checkout master && git pull
# we are pinned to a slightly older version of etcd until
#  metafora upgrades
#cd $GOPATH/src/github.com/coreos/etcd && git checkout master && git pull
#cd $GOPATH/src/github.com/coreos/go-etcd && git checkout master && git pull
cd $GOPATH/src/github.com/couchbaselabs/goforestdb && git checkout master && git pull
cd $GOPATH/src/github.com/dchest/siphash && git checkout master && git pull
cd $GOPATH/src/github.com/go-sql-driver/mysql && git checkout master && git pull
cd $GOPATH/src/github.com/gogo/protobuf && git checkout master && git pull
cd $GOPATH/src/github.com/golang/protobuf && git checkout master && git pull
cd $GOPATH/src/github.com/google/btree && git checkout master && git pull
cd $GOPATH/src/github.com/jmoiron/sqlx && git checkout master && git pull
cd $GOPATH/src/github.com/kr/pretty && git checkout master && git pull
cd $GOPATH/src/github.com/kr/pty && git checkout master && git pull
cd $GOPATH/src/github.com/kr/text && git checkout master && git pull
cd $GOPATH/src/github.com/leekchan/timeutil && git checkout master && git pull
cd $GOPATH/src/github.com/lytics/confl && git checkout master && git pull
cd $GOPATH/src/github.com/lytics/datemath && git checkout master && git pull
cd $GOPATH/src/github.com/lytics/dfa && git checkout master && git pull
#cd $GOPATH/src/github.com/lytics/grid && git checkout master && git pull
cd $GOPATH/src/github.com/lytics/metafora && git checkout master && git pull
cd $GOPATH/src/github.com/lytics/sereno && git checkout master && git pull
cd $GOPATH/src/github.com/mattbaird/elastigo && git checkout master && git pull
cd $GOPATH/src/github.com/mb0/glob && git checkout master && git pull
cd $GOPATH/src/github.com/nats-io/nats && git checkout master && git pull
cd $GOPATH/src/github.com/pborman/uuid && git checkout master && git pull
cd $GOPATH/src/github.com/rcrowley/go-metrics && git checkout master && git pull
cd $GOPATH/src/github.com/sony/sonyflake && git checkout master && git pull

# this one if updated breaks older versions of etcd
#cd $GOPATH/src/github.com/ugorji/go && git checkout master && git pull
cd $GOPATH/src/golang.org/x/net && git checkout master && git pull
cd $GOPATH/src/golang.org/x/oauth2 && git checkout master && git pull
cd $GOPATH/src/google.golang.org/api && git checkout master && git pull
cd $GOPATH/src/google.golang.org/cloud && git checkout master && git pull
cd $GOPATH/src/google.golang.org/grpc && git checkout master && git pull
# this one is a pain, don't update manually, c headers?
#cd $GOPATH/src/gopkg.in/mgo.v2 && git checkout master && git pull



#go get -u -v ./...

#glock save github.com/dataux/dataux

