#! /bin/sh

cd $GOPATH/src/cloud.google.com/go/ && git checkout master && git pull && git checkout v0.6.0
cd $GOPATH/src/github.com/PuerkitoBio/purell && git checkout master && git pull
cd $GOPATH/src/github.com/PuerkitoBio/purell && git checkout master && git pull
cd $GOPATH/src/github.com/araddon/dateparse && git checkout master && git pull
cd $GOPATH/src/github.com/araddon/gou && git checkout master && git pull
#cd $GOPATH/src/github.com/araddon/qlbridge && git checkout master && git pull
cd $GOPATH/src/github.com/asaskevich/govalidator && git checkout master && git pull
cd $GOPATH/src/github.com/beorn7/perks && git checkout master && git pull
cd $GOPATH/src/github.com/bitly/go-hostpool && git checkout master && git pull
# we are pinned to a slightly older version of etcd/bolt until
#  metafora upgrades
cd $GOPATH/src/github.com/boltdb/bolt && git checkout master && git pull
cd $GOPATH/src/github.com/cockroachdb/cmux && git checkout master && git pull

cd $GOPATH/src/github.com/coreos/etcd && git checkout master && git pull && git checkout v3.1.3

cd $GOPATH/src/github.com/coreos/go-semver && git checkout master && git pull
cd $GOPATH/src/github.com/coreos/go-systemd  && git checkout master && git pull
cd $GOPATH/src/github.com/coreos/pkg && git checkout master && git pull

cd $GOPATH/src/github.com/couchbaselabs/goforestdb && git checkout master && git pull
cd $GOPATH/src/github.com/dchest/siphash && git checkout master && git pull
cd $GOPATH/src/github.com/dgrijalva/jwt-go && git checkout master && git pull
cd $GOPATH/src/github.com/go-sql-driver/mysql && git checkout master && git pull
cd $GOPATH/src/github.com/ghodss/yaml && git checkout master && git pull
cd $GOPATH/src/github.com/gocql/gocql && git checkout master && git pull
cd $GOPATH/src/github.com/gogo/protobuf && git checkout master && git pull
cd $GOPATH/src/github.com/golang/protobuf && git checkout master && git pull
cd $GOPATH/src/github.com/golang/snappy && git checkout master && git pull
cd $GOPATH/src/github.com/google/btree && git checkout master && git pull
cd $GOPATH/src/github.com/google/gofuzz && git checkout master && git pull
cd $GOPATH/src/github.com/grpc-ecosystem/go-grpc-prometheus && git checkout master && git pull
cd $GOPATH/src/github.com/grpc-ecosystem/grpc-gateway && git checkout master && git pull && git checkout 84398b94e188ee336f307779b57b3aa91af7063c # check coreos/etcd for changes glide.yaml

cd $GOPATH/src/github.com/hailocab/go-hostpool && git checkout master && git pull
cd $GOPATH/src/github.com/hashicorp/go-immutable-radix && git checkout master && git pull
cd $GOPATH/src/github.com/hashicorp/go-memdb && git checkout master && git pull
cd $GOPATH/src/github.com/hashicorp/golang-lru && git checkout master && git pull
cd $GOPATH/src/github.com/jmoiron/sqlx && git checkout master && git pull
cd $GOPATH/src/github.com/jonboulle/clockwork && git checkout master && git pull
cd $GOPATH/src/github.com/kr/pretty && git checkout master && git pull
cd $GOPATH/src/github.com/kr/pty && git checkout master && git pull
cd $GOPATH/src/github.com/kr/text && git checkout master && git pull
cd $GOPATH/src/github.com/leekchan/timeutil && git checkout master && git pull
#cd $GOPATH/src/github.com/lytics/cloudstorage && git checkout master && git pull
cd $GOPATH/src/github.com/lytics/confl && git checkout master && git pull
cd $GOPATH/src/github.com/lytics/datemath && git checkout master && git pull
cd $GOPATH/src/github.com/lytics/dfa && git checkout master && git pull
#cd $GOPATH/src/github.com/lytics/grid && git checkout master && git pull
cd $GOPATH/src/github.com/lytics/metafora && git checkout master && git pull
cd $GOPATH/src/github.com/lytics/sereno && git checkout master && git pull
cd $GOPATH/src/github.com/mattbaird/elastigo && git checkout master && git pull
cd $GOPATH/src/github.com/mb0/glob && git checkout master && git pull
cd $GOPATH/src/github.com/nats-io/gnatsd && git checkout master && git pull
cd $GOPATH/src/github.com/nats-io/go-nats && git checkout master && git pull
cd $GOPATH/src/github.com/nats-io/nuid && git checkout master && git pull
cd $GOPATH/src/github.com/pborman/uuid && git checkout master && git pull
cd $GOPATH/src/github.com/prometheus/client_golang && git checkout master && git pull
cd $GOPATH/src/github.com/prometheus/client_model && git checkout master && git pull
cd $GOPATH/src/github.com/prometheus/common && git checkout master && git pull
cd $GOPATH/src/github.com/prometheus/procfs && git checkout master && git pull
cd $GOPATH/src/github.com/rcrowley/go-metrics && git checkout master && git pull
cd $GOPATH/src/github.com/sony/sonyflake && git checkout master && git pull
# this one if updated breaks older versions of etcd
cd $GOPATH/src/github.com/ugorji/go && git checkout master && git pull

#cd $GOPATH/src/github.com/uxiang90/probing && git checkout master && git pull


cd $GOPATH/src/github.com/googleapis/gax-go && git checkout master && git pull && git checkout da06d194a00e19ce00d9011a13931c3f6f6887c7
cd $GOPATH/src/golang.org/x/crypto && git checkout master && git pull
cd $GOPATH/src/golang.org/x/net && git checkout master && git pull
cd $GOPATH/src/golang.org/x/oauth2 && git checkout master && git pull
cd $GOPATH/src/google.golang.org/api && git checkout master && git pull  && git checkout dfa61ae24628a06502b9c2805d983b57e89399b5
cd $GOPATH/src/google.golang.org/genproto && git checkout master && git pull
cd $GOPATH/src/google.golang.org/grpc && git checkout master && git pull && git checkout v1.0.4 # check coreos/etcd for changes glide.yaml

cd $GOPATH/src/gopkg.in/inf.v0 && git checkout master && git pull
# this one is a pain, don't update manually, c headers?
#cd $GOPATH/src/gopkg.in/mgo.v2 && git checkout master && git pull
cd $GOPATH/src/gopkg.in/yaml.v2 && git checkout master && git pull

#echo "Fetch k8s.io"
cd $GOPATH/src/k8s.io/apimachinery && git checkout master && git pull # && git checkout xxx # ??
cd $GOPATH/src/k8s.io/client-go && git checkout master && git pull # && git checkout xxx # ??
cd $GOPATH/src/k8s.io/api && git checkout master && git pull # && git checkout xxx # ??
cd $GOPATH/src/k8s.io/kube-openapi && git checkout master && git pull # && git checkout xxx # ??

#go get -u -v ./...

#glock save github.com/dataux/dataux

