package models

import (
	"fmt"
	"io/ioutil"

	u "github.com/araddon/gou"
	"github.com/lytics/confl"
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
	SupressRecover bool              `json:"supress_recover"` // do we recover?
	LogLevel       string            `json:"log_level"`       // [debug,info,error,]
	Frontends      []*ListenerConfig `json:"frontends"`       // tcp listener configs
	Sources        []*SourceConfig   `json:"sources"`         // backend servers/sources (es, mysql etc)
	Schemas        []*SchemaConfig   `json:"schemas"`         // Schemas, each backend has 1 schema
	Nodes          []*NodeConfig     `json:"nodes"`           // list of nodes that host sources
}

// A Schema is a Virtual Schema, and may have multiple backend's
type SchemaConfig struct {
	Name        string      `json:"name"`    // Virtual Schema Name, must be unique
	Sources     []string    `json:"sources"` // List of sources , the names of the "Db" in source
	RulesConifg RulesConfig `json:"rules"`   // (optional) Routing rules
	Nodes       []string    `json:"-"`       // List of backend Servers
}

func (m *SchemaConfig) String() string {
	return fmt.Sprintf(`<schemaconf Name="%s" sources=[%v] />`, m.Name, m.Sources)
}

// Sources are storage/database/servers/csvfiles
//  - this represents a single source Type
//  - may have more than one node
//  - belongs to a Schema ( or schemas)
type SourceConfig struct {
	Name       string `json:"name"` // Name of this Source, ie a database schema
	SourceType string `json:"type"` // [mysql,elasticsearch,file]

	// Do we deprecate this?
	DownAfterNoAlive int    `json:"down_after_noalive"`
	IdleConns        int    `json:"idle_conns"`
	RWSplit          bool   `json:"rw_split"`
	User             string `json:"user"`
	Password         string `json:"password"`
	Master           string `json:"master"`
	Slave            string `json:"slave"`
}

func (m *SourceConfig) String() string {
	return fmt.Sprintf(`<sourceconf name="%s" type="%s" />`, m.Name, m.SourceType)
}

// A Node Is a physical server/instance of a service of type SourceType
//  and supporting one or more virtual schemas
type NodeConfig struct {
	Name       string `json:"name"`    // Node Name
	Address    string `json:"address"` // connection/ip info
	SourceType string `json:"type"`    // [mysql,elasticsearch,file]
}

func (m *NodeConfig) String() string {
	return fmt.Sprintf(`<nodeconfig name="%s" address="%s" type="%s" />`, m.Name, m.Address, m.SourceType)
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
