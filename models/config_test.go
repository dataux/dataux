package models

import (
	"testing"

	u "github.com/araddon/gou"
	"github.com/stretchr/testify/assert"
)

func init() {
	u.SetupLogging("debug")
	u.SetColorOutput()
}
func TestConfig(t *testing.T) {

	var configData = `

supress_recover = true
log_level = debug

# FrontEnd is our inbound tcp connection listener's
frontends [
  {
    type    : mysql
    address : "127.0.0.1:13307"
  }
]

# schemas
schemas : [
  {
    name : datauxtest
    sources : [ "mgo_datauxtest", "es_test", "csvlocal" ]
  }
]

# sources
sources : [
  {
    name : mgo_datauxtest
    type : mongo
  },
  {
    name : es_test
    type : elasticsearch
  },
  {
    name : csvlocal
    type : csv
  },
  {
    name : mysql_test
    type : mysql
  }
]


# List of nodes hosting data sources
nodes : [
  {
    name    : estest1
    type    : elasticsearch
    address : "http://localhost:9200"
  },
  {
    name    : mgotest1
    type    : mongo
    address : "localhost"
  },
  {
    name    : csvlocal1
    type    : csv
    address : "$GOPATH/src/github.com/dataux/dataux/data"
  }
]


`

	conf, err := LoadConfig(configData)
	assert.True(t, err == nil && conf != nil, "Must not error on parse of config: %v", err)

	assert.True(t, conf.LogLevel == "debug")
}
