// Copyright IBM Corp. 2016, 2026
// SPDX-License-Identifier: MPL-2.0

package plugin

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"
)

// Errors returned by the server-side plugin registry. Callers can use
// errors.Is to identify them programmatically instead of matching on error
// text.
var (
	// ErrPluginNotFound is returned when routing a request to a plugin name
	// that is not registered. The error wraps the requested name, so
	// errors.Is(err, ErrPluginNotFound) stays true across net/rpc boundaries
	// where only the error message survives.
	ErrPluginNotFound = errors.New("unknown plugin type")

	// ErrPluginAlreadyRegistered is returned when a plugin is registered
	// under a name that is already in use. The previous entry is left
	// untouched in this case instead of being silently overwritten.
	ErrPluginAlreadyRegistered = errors.New("plugin already registered")

	// ErrInvalidPluginName is returned when an empty name is used to
	// register or look up a plugin.
	ErrInvalidPluginName = errors.New("invalid plugin name")

	// ErrNilPlugin is returned when a nil Plugin implementation is handed
	// to the registry.
	ErrNilPlugin = errors.New("plugin implementation must not be nil")
)

// defaultPluginShutdownTimeout is the maximum time the server waits for
// plugins to drain after being notified of shutdown before the connection
// is forcibly closed.
const defaultPluginShutdownTimeout = 5 * time.Second

// PluginStatus is the lifecycle state of a plugin within the server-side
// registry.
type PluginStatus string

const (
	// PluginStatusRegistered means the plugin has been registered but the
	// server has not started serving connections yet.
	PluginStatusRegistered PluginStatus = "registered"

	// PluginStatusServing means the server is up and the plugin can be
	// routed to by name.
	PluginStatusServing PluginStatus = "serving"

	// PluginStatusStopped means the server has shut the plugin down.
	PluginStatusStopped PluginStatus = "stopped"
)

// PluginInfo is an immutable snapshot of a registry entry, returned by the
// registry's query methods. It never exposes the underlying plugin instance
// so callers cannot mutate registry state.
type PluginInfo struct {
	// Name is the unique name under which the plugin is registered.
	Name string

	// Status is the current lifecycle state of the plugin.
	Status PluginStatus
}

// registryEntry tracks a single plugin and its lifecycle state.
type registryEntry struct {
	plugin Plugin
	status PluginStatus
}

// PluginRegistry is the server-side registry of plugins served by a single
// plugin server.
//
// Registration validates that every plugin name is unique: registering a
// duplicate name returns ErrPluginAlreadyRegistered rather than replacing
// the previous entry. Routing by name goes through Lookup and an unknown
// name returns an error wrapping ErrPluginNotFound.
//
// The registry is in-memory only. A plugin process always starts with an
// empty registry, so restarting the server can never leave stale entries
// behind.
//
// A PluginRegistry is safe for concurrent use.
type PluginRegistry struct {
	mu      sync.RWMutex
	entries map[string]*registryEntry
}

// NewPluginRegistry creates an empty registry. If plugins is non-nil every
// entry is registered in sorted name order and a duplicate name, empty
// name or nil implementation makes construction fail with the same errors
// Register returns.
func NewPluginRegistry(plugins map[string]Plugin) (*PluginRegistry, error) {
	r := &PluginRegistry{entries: make(map[string]*registryEntry)}

	names := make([]string, 0, len(plugins))
	for name := range plugins {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		if err := r.Register(name, plugins[name]); err != nil {
			return nil, err
		}
	}

	return r, nil
}

// Register adds a plugin under the given name. It returns
// ErrInvalidPluginName for an empty name, ErrNilPlugin for a nil
// implementation and ErrPluginAlreadyRegistered if the name is already in
// use. On error the existing registry entry is preserved.
func (r *PluginRegistry) Register(name string, p Plugin) error {
	if name == "" {
		return ErrInvalidPluginName
	}
	if p == nil {
		return fmt.Errorf("%w: %q", ErrNilPlugin, name)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.entries[name]; exists {
		return fmt.Errorf("%w: %q", ErrPluginAlreadyRegistered, name)
	}

	r.entries[name] = &registryEntry{
		plugin: p,
		status: PluginStatusRegistered,
	}
	return nil
}

// Lookup routes a plugin name to its implementation. An empty or unknown
// name returns an error wrapping ErrPluginNotFound. The returned plugin is
// nil when the error is non-nil.
func (r *PluginRegistry) Lookup(name string) (Plugin, error) {
	if r == nil {
		return nil, fmt.Errorf("%w: %q", ErrPluginNotFound, name)
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	entry, ok := r.entries[name]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrPluginNotFound, name)
	}
	return entry.plugin, nil
}

// Get returns the registry entry for a name, or false if no plugin is
// registered under it.
func (r *PluginRegistry) Get(name string) (PluginInfo, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	entry, ok := r.entries[name]
	if !ok {
		return PluginInfo{}, false
	}
	return PluginInfo{Name: name, Status: entry.status}, true
}

// List returns a snapshot of every registered plugin sorted by name.
func (r *PluginRegistry) List() []PluginInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()

	infos := make([]PluginInfo, 0, len(r.entries))
	for name, entry := range r.entries {
		infos = append(infos, PluginInfo{Name: name, Status: entry.status})
	}
	sort.Slice(infos, func(i, j int) bool { return infos[i].Name < infos[j].Name })
	return infos
}

// Names returns the sorted list of registered plugin names.
func (r *PluginRegistry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	names := make([]string, 0, len(r.entries))
	for name := range r.entries {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// Len returns the number of registered plugins.
func (r *PluginRegistry) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.entries)
}

// setStatus updates the lifecycle state of a single plugin. Unknown names
// are ignored since the registry may legitimately have been queried while
// the server is winding down.
func (r *PluginRegistry) setStatus(name string, status PluginStatus) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if entry, ok := r.entries[name]; ok {
		entry.status = status
	}
}

// markServing transitions every registered plugin into the serving state.
func (r *PluginRegistry) markServing() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, entry := range r.entries {
		entry.status = PluginStatusServing
	}
}

// markStopped transitions every registered plugin into the stopped state.
func (r *PluginRegistry) markStopped() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, entry := range r.entries {
		entry.status = PluginStatusStopped
	}
}

// Shutdown notifies every registered plugin that the server is stopping and
// waits for them to exit. Plugins that implement io.Closer receive a Close
// call; all notifications are dispatched at the same time so one slow
// plugin cannot delay the others.
//
// The function waits at most timeout for all plugins to finish. Plugins that
// have not returned once the timeout elapses are abandoned: the server is
// expected to forcibly close its connections afterwards. A non-positive
// timeout falls back to defaultPluginShutdownTimeout. Errors returned by
// individual Close calls are joined and surfaced, but never block the
// shutdown of the remaining plugins.
func (r *PluginRegistry) Shutdown(timeout time.Duration) error {
	if timeout <= 0 {
		timeout = defaultPluginShutdownTimeout
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Snapshot the plugins to close under the lock so shutdown does not
	// race with registration.
	r.mu.RLock()
	closers := make([]io.Closer, 0, len(r.entries))
	for _, entry := range r.entries {
		if closer, ok := entry.plugin.(io.Closer); ok {
			closers = append(closers, closer)
		}
	}
	r.mu.RUnlock()

	var (
		wg    sync.WaitGroup
		errMu sync.Mutex
		errs  []error
	)
	addErr := func(err error) {
		errMu.Lock()
		errs = append(errs, err)
		errMu.Unlock()
	}
	for _, closer := range closers {
		wg.Add(1)
		go func(c io.Closer) {
			defer wg.Done()
			// A misbehaving plugin must not be able to panic the shutdown
			// goroutine and wedge the whole server.
			defer func() {
				if rec := recover(); rec != nil {
					addErr(fmt.Errorf("plugin close panicked: %v", rec))
				}
			}()
			if err := c.Close(); err != nil {
				addErr(err)
			}
		}(closer)
	}

	// Closed once every plugin has returned, or abandoned on timeout.
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		addErr(fmt.Errorf(
			"plugin shutdown timed out after %s; forcing server stop", timeout))
	}

	r.markStopped()
	return errors.Join(errs...)
}
