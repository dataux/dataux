package models

import (
	"fmt"
	u "github.com/araddon/gou"
	"github.com/bmizerany/assert"
	"reflect"
	"testing"
)

func init() {
	u.SetupLogging("debug")
	u.SetColorOutput()
}
func TestConfig(t *testing.T) {

	var configData = `
addr : "127.0.0.1:4000"
user : root
# password : ""
log_level : error

backends [
  {
    name : node1 
    down_after_noalive : 300
    idle_conns : 16
    rw_split : true
    user : root
    #password: ""
    master : "127.0.0.1:3306"
    slave : "127.0.0.1:4306"
  },
  {
    name : node2
    user: root
    master : "127.0.0.1:3307"
  },
  {
    name : node3 
    down_after_noalive : 300
    idle_conns : 16
    rw_split: false
    user : root
    master : "127.0.0.1:3308"
  }
]

# schemas
schemas : [
  {
    db : mixer
    backends : ["node1", "node2", "node3"]
    backend_type : mysql
    # list of rules
    rules : {
      default : node1
      # shards
      shard : [
        {
          table : mixer_test_shard_hash
          key : id
          backends: ["node1", "node2", "node3"]
          type : hash
        },
        {
          table: mixer_test_shard_range
          key: id
          type: range
          backends: [ node2, node3 ]
          range: "-10000-"
        }
      ]
    }
  }
]
`

	conf, err := LoadConfig(configData)
	assert.Tf(t, err == nil && conf != nil, "Must not error on parse of config: %v", err)

	if len(conf.Backends) != 3 {
		t.Fatal(len(conf.Backends))
	}

	if len(conf.Schemas) != 1 {
		t.Fatal(len(conf.Schemas))
	}

	testNode := BackendConfig{
		Name:             "node1",
		DownAfterNoAlive: 300,
		IdleConns:        16,
		RWSplit:          true,

		User:     "root",
		Password: "",

		Master: "127.0.0.1:3306",
		Slave:  "127.0.0.1:4306",
	}

	if !reflect.DeepEqual(conf.Backends[0], &testNode) {
		t.Fatalf("node1 must equal %v", fmt.Sprintf("%v\n", conf.Backends[0]))
	}

	testNode_2 := BackendConfig{
		Name:   "node2",
		User:   "root",
		Master: "127.0.0.1:3307",
	}

	if !reflect.DeepEqual(conf.Backends[1], &testNode_2) {
		t.Fatal("node2 must equal")
	}

	testShard_1 := ShardConfig{
		Table:    "mixer_test_shard_hash",
		Key:      "id",
		Backends: []string{"node1", "node2", "node3"},
		Type:     "hash",
	}
	if !reflect.DeepEqual(conf.Schemas[0].RulesConifg.ShardRule[0], testShard_1) {
		t.Fatal("ShardConfig0 must equal")
	}

	testShard_2 := ShardConfig{
		Table:    "mixer_test_shard_range",
		Key:      "id",
		Backends: []string{"node2", "node3"},
		Type:     "range",
		Range:    "-10000-",
	}
	if !reflect.DeepEqual(conf.Schemas[0].RulesConifg.ShardRule[1], testShard_2) {
		t.Fatal("ShardConfig1 must equal")
	}

	if 2 != len(conf.Schemas[0].RulesConifg.ShardRule) {
		t.Fatal("ShardRule must 2")
	}

	testRules := RulesConfig{
		Default:   "node1",
		ShardRule: []ShardConfig{testShard_1, testShard_2},
	}
	if !reflect.DeepEqual(conf.Schemas[0].RulesConifg, testRules) {
		t.Fatal("RulesConfig must equal")
	}

	testSchema := SchemaConfig{
		DB:          "mixer",
		BackendType: "mysql",
		Backends:    []string{"node1", "node2", "node3"},
		RulesConifg: testRules,
	}

	if !reflect.DeepEqual(conf.Schemas[0], &testSchema) {
		t.Fatal("schema must equal")
	}

	if conf.LogLevel != "error" {
		t.Fatal("Top Config not equal.")
	}
}
