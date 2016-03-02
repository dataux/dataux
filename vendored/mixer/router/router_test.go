package router

import (
	"github.com/dataux/dataux/models"
	"testing"
)

func TestParseRule(t *testing.T) {
	var s = `

frontends [
  {
    name : test1
    backends : ["node1", "node2", "node3"]
    backend_type : mysql
  }
]

schemas : [
  {
    name : test1
    backends : ["node1", "node2", "node3"]
    backend_type : mysql
  }
]

# list of routing rules
rules [
  {
    schema  : test1
    default : node1
    # shards
    shard : [
      {
        table : mixer_test_shard_hash
        key : id
        backends: [ "node2", "node3"]
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
]
`
	conf, err := models.LoadConfig(s)
	if err != nil {
		t.Fatalf("Should load config: %v", err)
	}
	ctx := models.NewServerCtx(conf)
	rt, err := NewRouter(conf, ctx.Schema("test1"))
	if err != nil {
		t.Fatal(err)
	}
	if rt.DefaultRule.Nodes[0] != "node1" {
		t.Fatal("default rule parse not correct.")
	}

	hashRule := rt.GetRule("mixer_test_shard_hash")
	if hashRule.Type != HashRuleType {
		t.Fatal(hashRule.Type)
	}

	if len(hashRule.Nodes) != 2 || hashRule.Nodes[0] != "node2" || hashRule.Nodes[1] != "node3" {
		t.Fatal("parse nodes not correct.")
	}

	if n := hashRule.FindNode(uint64(11)); n != "node3" {
		t.Fatal(n)
	}

	rangeRule := rt.GetRule("mixer_test_shard_range")
	if rangeRule.Type != RangeRuleType {
		t.Fatal(rangeRule.Type)
	}

	if n := rangeRule.FindNode(10000 - 1); n != "node2" {
		t.Fatal(n)
	}

	defaultRule := rt.GetRule("mixer_defaultRule_table")
	if defaultRule == nil {
		t.Fatal("must not nil")
	}

	if defaultRule.Type != DefaultRuleType {
		t.Fatal(defaultRule.Type)
	}

	if defaultRule.Shard == nil {
		t.Fatal("nil error")
	}

	if n := defaultRule.FindNode(11); n != "node1" {
		t.Fatal(n)
	}
}
