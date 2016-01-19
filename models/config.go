package models

import (
	"io/ioutil"

	u "github.com/araddon/gou"
	"github.com/lytics/confl"

	"github.com/araddon/qlbridge/schema"
)

var (
	_ = u.EMPTY
)

// Read a Confl configured file
func LoadConfigFromFile(filename string) (*Config, error) {
	var c Config
	confBytes, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	if _, err = confl.Decode(string(confBytes), &c); err != nil {
		return nil, err
	}
	//u.Debug(string(confBytes))
	return &c, nil
}

func LoadConfig(conf string) (*Config, error) {
	var c Config
	if _, err := confl.Decode(conf, &c); err != nil {
		return nil, err
	}
	return &c, nil
}

// Overall DataUX Server config made up of blocks
//   1) Frontend Listeners (protocols)
//   2) Sources (types of backends such as elasticsearch, mysql, mongo, ...)
//   3) Virtual Schemas
//   4) list of server/nodes for Sources
type Config struct {
	SupressRecover bool                   `json:"supress_recover"` // do we recover?
	LogLevel       string                 `json:"log_level"`       // [debug,info,error,]
	Frontends      []*ListenerConfig      `json:"frontends"`       // tcp listener configs
	Sources        []*schema.SourceConfig `json:"sources"`         // backend servers/sources (es, mysql etc)
	Schemas        []*schema.SchemaConfig `json:"schemas"`         // Schemas, each backend has 1 schema
	Nodes          []*schema.NodeConfig   `json:"nodes"`           // list of nodes that host sources
	Rules          *RulesConfig
}

// Frontend Listener to listen for inbound traffic on
// specific protocola or transport
type ListenerConfig struct {
	Type     string `json:"type"`     // [mysql,mongo,mc,postgres,etc]
	Addr     string `json:"address"`  // net.Conn compatible ip/dns address
	User     string `json:"user"`     // user to talk to backend with
	Password string `json:"password"` // optional pwd for backend
}

type RulesConfig struct {
	Schema    string        `json:"default"`
	Default   string        `json:"default"`
	ShardRule []ShardConfig `json:"shard"`
}

type ShardConfig struct {
	Table string   `json:"table"`
	Key   string   `json:"key"`
	Nodes []string `json:"nodes"`
	Type  string   `json:"type"`
	Range string   `json:"range"`
}
