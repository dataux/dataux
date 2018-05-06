package grid

import (
	"runtime"
	"time"
)

// Logger hides the logging function Printf behind a simple
// interface so libraries such as logrus can be used.
type Logger interface {
	Printf(string, ...interface{})
}

// ClientCfg where the only required argument is Namespace,
// other fields with their zero value will receive defaults.
type ClientCfg struct {
	// Namespace of grid.
	Namespace string
	// Timeout for communication with etcd, and internal gossip.
	Timeout time.Duration
	// PeersRefreshInterval for polling list of peers in etcd.
	PeersRefreshInterval time.Duration
	// ConnectionsPerPeer sets the number gRPC connections to
	// establish to each remote. Default is max(1, numCPUs/2).
	// More connections allow for more messages per second,
	// but increases the number of file-handles used.
	ConnectionsPerPeer int
	// Logger optionally used for logging, default is to not log.
	Logger Logger
}

// setClientCfgDefaults for those fields that have their zero value.
func setClientCfgDefaults(cfg *ClientCfg) {
	if cfg.PeersRefreshInterval == 0 {
		cfg.PeersRefreshInterval = 2 * time.Second
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 10 * time.Second
	}
	if cfg.ConnectionsPerPeer == 0 {
		cfg.ConnectionsPerPeer = maxInt(1, runtime.NumCPU()/2)
	}
}

// ServerCfg where the only required argument is Namespace,
// other fields with their zero value will receive defaults.
type ServerCfg struct {
	// Namespace of grid.
	Namespace string
	// DisalowLeadership to prevent leader from running on a node.
	DisalowLeadership bool
	// Timeout for communication with etcd, and internal gossip.
	Timeout time.Duration
	// LeaseDuration for data in etcd.
	LeaseDuration time.Duration
	// Logger optionally used for logging, default is to not log.
	Logger Logger
}

// setServerCfgDefaults for those fields that have their zero value.
func setServerCfgDefaults(cfg *ServerCfg) {
	if cfg.Timeout == 0 {
		cfg.Timeout = 10 * time.Second
	}
	if cfg.LeaseDuration == 0 {
		cfg.LeaseDuration = 60 * time.Second
	}
}

func maxInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
