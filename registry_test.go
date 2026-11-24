// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

package plugin

import (
	"errors"
	"sync"
	"testing"
	"time"
)

// closerPlugin implements both GRPCPlugin and io.Closer so shutdown
// notification can be observed through the full gRPC server path.
type closerPlugin struct {
	*testGRPCInterfacePlugin

	closeCalled chan struct{}
	closeErr    error
	release     chan struct{}
	once        sync.Once
}

func newCloserPlugin(release chan struct{}, closeErr error) *closerPlugin {
	return &closerPlugin{
		testGRPCInterfacePlugin: new(testGRPCInterfacePlugin),
		closeCalled:             make(chan struct{}),
		closeErr:                closeErr,
		release:                 release,
	}
}

func (p *closerPlugin) Close() error {
	p.once.Do(func() { close(p.closeCalled) })
	if p.release != nil {
		<-p.release
	}
	return p.closeErr
}

func TestPluginRegistry_RegisterDuplicate(t *testing.T) {
	r := &PluginRegistry{entries: make(map[string]*registryEntry)}

	if err := r.Register("team-a/foo", new(testInterfacePlugin)); err != nil {
		t.Fatalf("first registration failed: %s", err)
	}

	// A sibling implementation sharing a name must not silently replace
	// the first one.
	second := new(testInterfacePlugin)
	err := r.Register("team-a/foo", second)
	if !errors.Is(err, ErrPluginAlreadyRegistered) {
		t.Fatalf("expected ErrPluginAlreadyRegistered, got: %v", err)
	}

	got, err := r.Lookup("team-a/foo")
	if err != nil {
		t.Fatalf("lookup failed: %s", err)
	}
	if got == second {
		t.Fatal("duplicate registration overwrote the existing plugin")
	}
}

func TestPluginRegistry_RegisterValidation(t *testing.T) {
	r := &PluginRegistry{entries: make(map[string]*registryEntry)}

	if err := r.Register("", new(testInterfacePlugin)); !errors.Is(err, ErrInvalidPluginName) {
		t.Fatalf("expected ErrInvalidPluginName, got: %v", err)
	}
	if err := r.Register("nil", nil); !errors.Is(err, ErrNilPlugin) {
		t.Fatalf("expected ErrNilPlugin, got: %v", err)
	}
}

func TestPluginRegistry_NewFromMapDuplicate(t *testing.T) {
	// Go maps cannot express duplicate keys, but a caller-supplied
	// registry plus PluginSet can: exercise that conflict path through
	// the same Register logic.
	r, err := NewPluginRegistry(map[string]Plugin{
		"team-a/foo": new(testInterfacePlugin),
		"team-a/bar": new(testInterfacePlugin),
	})
	if err != nil {
		t.Fatalf("construction failed: %s", err)
	}

	err = r.Register("team-a/foo", new(testInterfacePlugin))
	if !errors.Is(err, ErrPluginAlreadyRegistered) {
		t.Fatalf("expected ErrPluginAlreadyRegistered, got: %v", err)
	}
}

func TestPluginRegistry_LookupUnknown(t *testing.T) {
	r, err := NewPluginRegistry(map[string]Plugin{
		"team-a/foo": new(testInterfacePlugin),
		"team-a/bar": new(testInterfacePlugin),
	})
	if err != nil {
		t.Fatalf("construction failed: %s", err)
	}

	// Similar-looking prefixes must not match: routing is exact-name.
	if _, err := r.Lookup("team-a/fo"); !errors.Is(err, ErrPluginNotFound) {
		t.Fatalf("expected ErrPluginNotFound, got: %v", err)
	}
	if _, err := r.Lookup("team-a/foo "); !errors.Is(err, ErrPluginNotFound) {
		t.Fatalf("expected ErrPluginNotFound for padded name, got: %v", err)
	}
	if _, err := r.Lookup(""); !errors.Is(err, ErrPluginNotFound) {
		t.Fatalf("expected ErrPluginNotFound for empty name, got: %v", err)
	}

	// The exact name must still route to the right implementation.
	p, err := r.Lookup("team-a/foo")
	if err != nil {
		t.Fatalf("lookup failed: %s", err)
	}
	if p == nil {
		t.Fatal("expected non-nil plugin")
	}

	// A nil registry behaves like an empty one instead of panicking.
	var nilRegistry *PluginRegistry
	if _, err := nilRegistry.Lookup("anything"); !errors.Is(err, ErrPluginNotFound) {
		t.Fatalf("expected ErrPluginNotFound from nil registry, got: %v", err)
	}
}

func TestPluginRegistry_Query(t *testing.T) {
	r, err := NewPluginRegistry(map[string]Plugin{
		"b": new(testInterfacePlugin),
		"a": new(testInterfacePlugin),
	})
	if err != nil {
		t.Fatalf("construction failed: %s", err)
	}

	if r.Len() != 2 {
		t.Fatalf("expected len 2, got %d", r.Len())
	}
	if names := r.Names(); names[0] != "a" || names[1] != "b" {
		t.Fatalf("expected sorted names [a b], got %v", names)
	}

	infos := r.List()
	if len(infos) != 2 || infos[0].Name != "a" || infos[0].Status != PluginStatusRegistered {
		t.Fatalf("unexpected snapshot: %+v", infos)
	}

	r.markServing()
	if info, ok := r.Get("a"); !ok || info.Status != PluginStatusServing {
		t.Fatalf("expected serving status, got %+v ok=%v", info, ok)
	}

	r.markStopped()
	for _, info := range r.List() {
		if info.Status != PluginStatusStopped {
			t.Fatalf("plugin %q not stopped: %s", info.Name, info.Status)
		}
	}

	if _, ok := r.Get("missing"); ok {
		t.Fatal("expected missing plugin to report ok=false")
	}
}

func TestPluginRegistry_ShutdownNotifiesAndWaits(t *testing.T) {
	release := make(chan struct{})
	closing := newCloserPlugin(release, nil)
	plain := new(testInterfacePlugin)

	r, err := NewPluginRegistry(map[string]Plugin{
		"closing": closing,
		"plain":   plain,
	})
	if err != nil {
		t.Fatalf("construction failed: %s", err)
	}

	done := make(chan error, 1)
	go func() { done <- r.Shutdown(time.Second) }()

	// Every closer must be notified, even while one of them is still
	// draining.
	select {
	case <-closing.closeCalled:
	case <-time.After(time.Second):
		t.Fatal("plugin was not notified of shutdown")
	}

	// Shutdown must wait for the notified plugin rather than returning
	// early.
	select {
	case <-done:
		t.Fatal("Shutdown returned before plugins exited")
	default:
	}

	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("unexpected shutdown error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Shutdown did not return after plugins exited")
	}

	for _, info := range r.List() {
		if info.Status != PluginStatusStopped {
			t.Fatalf("plugin %q not marked stopped, got %s", info.Name, info.Status)
		}
	}
}

func TestPluginRegistry_ShutdownTimeoutForcesReturn(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	hung := newCloserPlugin(release, nil)

	r, err := NewPluginRegistry(map[string]Plugin{"hung": hung})
	if err != nil {
		t.Fatalf("construction failed: %s", err)
	}

	start := time.Now()
	err = r.Shutdown(25 * time.Millisecond)
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
	if time.Since(start) > time.Second {
		t.Fatal("Shutdown did not enforce the timeout")
	}
	if info, _ := r.Get("hung"); info.Status != PluginStatusStopped {
		t.Fatalf("expected stopped status after forced shutdown, got %s", info.Status)
	}
}

func TestPluginRegistry_ShutdownPropagatesCloseErrors(t *testing.T) {
	wantErr := errors.New("close failed")
	closing := newCloserPlugin(nil, wantErr)

	r, err := NewPluginRegistry(map[string]Plugin{"closing": closing})
	if err != nil {
		t.Fatalf("construction failed: %s", err)
	}

	if err := r.Shutdown(time.Second); !errors.Is(err, wantErr) {
		t.Fatalf("expected close error to be joined, got: %v", err)
	}
}

func TestPluginRegistry_ShutdownNoClosers(t *testing.T) {
	r, err := NewPluginRegistry(map[string]Plugin{
		"plain": new(testInterfacePlugin),
	})
	if err != nil {
		t.Fatalf("construction failed: %s", err)
	}
	if err := r.Shutdown(time.Second); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestPluginRegistry_ConcurrentAccess(t *testing.T) {
	r := &PluginRegistry{entries: make(map[string]*registryEntry)}

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(3)
		go func(i int) {
			defer wg.Done()
			_ = r.Register(uniqueName(i), new(testInterfacePlugin))
		}(i)
		go func() {
			defer wg.Done()
			_ = r.List()
		}()
		go func(i int) {
			defer wg.Done()
			_, _ = r.Lookup(uniqueName(i))
		}(i)
	}
	wg.Wait()

	if r.Len() != 50 {
		t.Fatalf("expected 50 entries, got %d", r.Len())
	}
}

func uniqueName(i int) string {
	return "plugin-" + string(rune('a'+i%26)) + "-" + time.Duration(i).String()
}
