package cloudstorage

import (
	"fmt"
	"sync"
)

var (
	// global registry lock
	registryMu sync.RWMutex
	// store provider registry
	storeProviders = make(map[string]StoreProvider)
)

// StoreProvider a provider function for creating New Stores
type StoreProvider func(*Config) (Store, error)

// Register adds a store type provider.
func Register(storeType string, provider StoreProvider) {
	registryMu.Lock()
	defer registryMu.Unlock()
	if _, ok := storeProviders[storeType]; ok {
		panic(fmt.Sprintf("Cannot provide duplicate store %q", storeType))
	}
	storeProviders[storeType] = provider
}
