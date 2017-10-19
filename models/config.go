package models

import (
	"io/ioutil"
	"os"

	"github.com/araddon/qlbridge/schema"
	"github.com/lytics/confl"
)

// LoadConfigFromFile Read a Confl formatted config file from disk
func LoadConfigFromFile(filename string) (*Config, error) {
	var c Config
	confBytes, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	if _, err = confl.Decode(os.ExpandEnv(string(confBytes)), &c); err != nil {
		return nil, err
	}
	return &c, nil
}

// LoadConfig load a confl formatted file from string (assumes came)
//  from file or passed in
func LoadConfig(conf string) (*Config, error) {
	var c Config
	if _, err := confl.Decode(os.ExpandEnv(conf), &c); err != nil {
		return nil, err
	}
	return &c, nil
}

type (
	// Config for DataUX Server config made up of blocks
	// 1) Frontend Listeners (protocols)
	// 2) Sources (types of backends such as elasticsearch, mysql, mongo, ...)
	// 3) Schemas:  n number of sources can create a "Virtual Schema"
	// 4) list of server/nodes for Sources
	// 5) etcd coordinators hosts
	Config struct {
		SupressRecover bool                   `json:"supress_recover"` // do we recover?
		WorkerCt       int                    `json:"worker_ct"`       // 4 how many worker nodes on this instance
		LogLevel       string                 `json:"log_level"`       // [debug,info,error,]
		Etcd           []string               `json:"etcd"`            // list of etcd servers http://127.0.0.1:2379,http://127.0.0.1:2380
		Frontends      []*ListenerConfig      `json:"frontends"`       // tcp listener configs
		Sources        []*schema.ConfigSource `json:"sources"`         // backend servers/sources (es, mysql etc)
		Schemas        []*schema.ConfigSchema `json:"schemas"`         // Schemas, each backend has 1 schema
		Nodes          []*schema.ConfigNode   `json:"nodes"`           // list of nodes that host sources
		Rules          *RulesConfig           `json:"rules"`           // rules for routing
	}
	// ListenerConfig Frontend Listener to listen for inbound
	// traffic on specific protocol aka transport (mysql)
	ListenerConfig struct {
		Type     string `json:"type"`     // named protocol type [mysql,mongo,mc,postgres,etc]
		Addr     string `json:"address"`  // net.Conn compatible ip/dns address
		User     string `json:"user"`     // user to talk to backend with
		Password string `json:"password"` // optional pwd for backend
	}

	// RulesConfig
	RulesConfig struct {
		Schema    string        `json:"schema"`
		Default   string        `json:"default"`
		ShardRule []ShardConfig `json:"shard"`
	}
	// ShardConfig
	ShardConfig struct {
		Table string   `json:"table"`
		Key   string   `json:"key"`
		Nodes []string `json:"nodes"`
		Type  string   `json:"type"`
		Range string   `json:"range"`
	}
)

// DistributedMode  Does this config operate in distributed mode?
func (c *Config) DistributedMode() bool {
	if len(c.Etcd) == 0 {
		return false
	}
	return true
}
