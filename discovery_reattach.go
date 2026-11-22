// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

package plugin

import (
	"context"
	"fmt"
)

// ReattachConfigFromRecord validates the record through the registry and
// builds the ReattachConfig needed by NewClient. It is the supported way to
// reattach to a plugin recorded by the versioned lifecycle: the registry
// re-checks identity, generation, state, expiry and liveness before handing
// out the config, so a reused PID or port can never bind to a stale or
// upgraded instance.
//
// The returned config's PID is only populated when the record's RunnerID is a
// numeric process ID. For custom runners, callers set ReattachFunc on the
// returned config before passing it to NewClient.
func ReattachConfigFromRecord(ctx context.Context, registry *DiscoveryRegistry, identity PluginIdentity) (*ReattachConfig, error) {
	rec, err := registry.Reattach(ctx, identity)
	if err != nil {
		return nil, err
	}

	cfg, err := rec.ReattachConfig()
	if err != nil {
		return nil, err
	}
	cfg.DiscoveryRecord = rec
	if cfg.Pid == 0 && cfg.ReattachFunc == nil {
		return nil, fmt.Errorf("record runner ID %q is not a numeric PID: set ReattachFunc before reattaching", rec.RunnerID)
	}
	return cfg, nil
}

// ReattachConfigForGeneration is ReattachConfigFromRecord with a pinned
// generation, for callers that cached a specific record and must not be
// redirected to a newer instance.
func ReattachConfigForGeneration(ctx context.Context, registry *DiscoveryRegistry, identity PluginIdentity, generation uint64) (*ReattachConfig, error) {
	rec, err := registry.ReattachGeneration(ctx, identity, generation)
	if err != nil {
		return nil, err
	}

	cfg, err := rec.ReattachConfig()
	if err != nil {
		return nil, err
	}
	cfg.DiscoveryRecord = rec
	if cfg.Pid == 0 && cfg.ReattachFunc == nil {
		return nil, fmt.Errorf("record runner ID %q is not a numeric PID: set ReattachFunc before reattaching", rec.RunnerID)
	}
	return cfg, nil
}
