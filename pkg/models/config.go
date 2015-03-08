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
	Backends       []*BackendConfig  `json:"backends"`        // backend servers (es, mysql etc)
	Schemas        []*SchemaConfig   `json:"schemas"`         // virtual schema
}

// Backends are storage/database/servers/csvfiles
// eventually this should come from a coordinator (etcd/zk/etc)
type BackendConfig struct {
	Name             string `json:"name"`
	BackendType      string `json:"backend_type"`
	Address          string `json:"address"` // If we don't need Per-Node info
	DownAfterNoAlive int    `json:"down_after_noalive"`
	IdleConns        int    `json:"idle_conns"`
	RWSplit          bool   `json:"rw_split"`
	User             string `json:"user"`
	Password         string `json:"password"`
	Master           string `json:"master"`
	Slave            string `json:"slave"`
}

func (m *BackendConfig) String() string {
	return fmt.Sprintf("<backendconf %s type=%s />", m.Name, m.BackendType)
}

// Frontend inbound protocol/transport
type ListenerConfig struct {
	Type     string `json:"type"`     // [mysql,mongo,mc,postgres,etc]
	DB       string `json:"db"`       // the Virtual Db name?   do we need this?
	Addr     string `json:"addr"`     // net.Conn compatible ip/dns address
	User     string `json:"user"`     // user to talk to backend with
	Password string `json:"password"` // optional pwd for backend
}

type SchemaConfig struct {
	BackendType string       `json:"backend_type"` // [mysql,elasticsearch,file]
	DB          string       `json:"db"`           // Database Name, must be unique
	Backends    []string     `json:"backends"`     // List of backend Servers
	RulesConifg RulesConfig  `json:"rules"`        // (optional) Routing rules
	Properties  u.JsonHelper `json:"properties"`   // Additional properties
}

func (m *SchemaConfig) String() string {
	return fmt.Sprintf("<schemaconf db=%s type=%s backends=%v />", m.DB, m.BackendType, m.Backends)
}

type RulesConfig struct {
	Default   string        `json:"default"`
	ShardRule []ShardConfig `json:"shard"`
}

type ShardConfig struct {
	Table    string   `json:"table"`
	Key      string   `json:"key"`
	Backends []string `json:"backends"`
	Type     string   `json:"type"`
	Range    string   `json:"range"`
}
