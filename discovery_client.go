// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

package plugin

// discoveryResources returns the filesystem paths owned exclusively by a
// claimed client instance. Currently this is just the per-client Unix socket
// directory, which the legacy Kill path removes directly; recording it on the
// lifecycle record lets bounded cleanup reclaim it even if the host restarts.
func discoveryResources(cfg *UnixSocketConfig) []string {
	if cfg == nil || cfg.socketDir == "" {
		return nil
	}
	return []string{cfg.socketDir}
}
