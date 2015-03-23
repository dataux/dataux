package models

import (
	"fmt"
	u "github.com/araddon/gou"
	"github.com/lytics/confl"
	"io/ioutil"
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

	return &c, nil
}

func LoadConfig(conf string) (*Config, error) {
	var c Config
	if _, err := confl.Decode(conf, &c); err != nil {
		return nil, err
	}
	return &c, nil
}

// Master config
type Config struct {
	SupressRecover bool              `json:"supress_recover"` // do we recover?
	LogLevel       string            `json:"log_level"`       // [debug,info,error,]
	Frontends      []*ListenerConfig `json:"frontends"`       // tcp listener configs
	Sources        []*SourceConfig   `json:"sources"`         // backend servers/sources (es, mysql etc)
	Schemas        []*SchemaConfig   `json:"schemas"`         // Schemas, each backend has 1 schema
}

// Backends are storage/database/servers/csvfiles
//  eventually this should come from a coordinator (etcd/zk/etc)
//  - this represents a single server/node and may vary between nodes
type SourceConfig struct {
	Name             string `json:"name"`
	SourceType       string `json:"source_type"` // [mysql,elasticsearch,file]
	Address          string `json:"address"`     // If we don't need Per-Node info
	DownAfterNoAlive int    `json:"down_after_noalive"`
	IdleConns        int    `json:"idle_conns"`
	RWSplit          bool   `json:"rw_split"`
	User             string `json:"user"`
	Password         string `json:"password"`
	Master           string `json:"master"`
	Slave            string `json:"slave"`
	DataSource       DataSource
}

func (m *SourceConfig) String() string {
	return fmt.Sprintf("<sourceconf %s address=%s type=%s/>", m.Name, m.Address, m.SourceType)
}

// Frontend inbound protocol/transport
type ListenerConfig struct {
	Type     string `json:"type"`     // [mysql,mongo,mc,postgres,etc]
	DB       string `json:"db"`       // the Virtual Db name?   do we need this?
	Addr     string `json:"addr"`     // net.Conn compatible ip/dns address
	User     string `json:"user"`     // user to talk to backend with
	Password string `json:"password"` // optional pwd for backend
}

//
type SchemaConfig struct {
	SourceType  string       `json:"source_type"`  // [mysql,elasticsearch,file]
	Address     string       `json:"address"`      // If we don't need Per-Node info
	DB          string       `json:"db"`           // Database Name, must be unique
	Alias       string       `json:"alias"`        // Virtual Database Name, must be unique, use db if not provided
	Nodes       []string     `json:"source_nodes"` // List of backend Servers
	RulesConifg RulesConfig  `json:"rules"`        // (optional) Routing rules
	Properties  u.JsonHelper `json:"properties"`   // Additional properties
}

func (m *SchemaConfig) String() string {
	return fmt.Sprintf("<schemaconf db=%s type=%s source_nodes=%v />", m.DB, m.SourceType, m.Nodes)
}

type RulesConfig struct {
	Default   string        `json:"default"`
	ShardRule []ShardConfig `json:"shard"`
}

type ShardConfig struct {
	Table string   `json:"table"`
	Key   string   `json:"key"`
	Nodes []string `json:"source_nodes"`
	Type  string   `json:"type"`
	Range string   `json:"range"`
}
