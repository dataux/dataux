package models

import (
	u "github.com/araddon/gou"
	"strings"
	"sync"
)

var (
	_ = u.EMPTY

	listenerMu    sync.Mutex
	listenerFuncs = make(map[string]ListenerAndHandler)
)

// A listener is a protocol specific, and transport specific
//  reader of requests which will be routed to a handler
type Listener interface {
	Run(handle ConnectionHandle, stop chan bool) error
	Close() error
}

type ListenerInit func(*ListenerConfig, *Config) (Listener, error)

func ListenerRegister(name string, fn ListenerInit, connHandle ConnectionHandle) {
	listenerMu.Lock()
	defer listenerMu.Unlock()
	name = strings.ToLower(name)
	//u.Debugf("registering listener [%s] ", name)
	listenerFuncs[name] = ListenerAndHandler{fn, connHandle}
}

func Listeners() map[string]ListenerAndHandler {
	return listenerFuncs
}

type ListenerAndHandler struct {
	ListenerInit
	ConnectionHandle
}
