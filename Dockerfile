FROM golang:1.9

RUN \
  go get -u -v github.com/golang/dep/cmd/dep

COPY . ${GOPATH}/src/github.com/dataux/dataux/
WORKDIR ${GOPATH}/src/github.com/dataux/dataux/

RUN \
  dep ensure -update -v && \
  go build

ENTRYPOINT [ "./dataux" ]
