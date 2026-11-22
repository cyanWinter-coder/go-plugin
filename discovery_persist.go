// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

package plugin

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// recordFileSuffix is the extension of every discovery record on disk.
const recordFileSuffix = ".plugin-discovery.json"

// storedRecord is the on-disk representation of a DiscoveryRecord. Function
// fields (Liveness, Cleanup) are never persisted: they are supplied by the
// registry configuration on load.
type storedRecord struct {
	Identity         PluginIdentity `json:"identity"`
	Generation       uint64         `json:"generation"`
	State            string         `json:"state"`
	RunnerID         string         `json:"runner_id,omitempty"`
	Network          string         `json:"network,omitempty"`
	Address          string         `json:"address,omitempty"`
	Resources        []string       `json:"resources,omitempty"`
	CreatedAt        time.Time      `json:"created_at"`
	CommittedAt      time.Time      `json:"committed_at,omitempty"`
	ExpiresAt        time.Time      `json:"expires_at,omitempty"`
	TombstonedAt     time.Time      `json:"tombstoned_at,omitempty"`
	CleanupAttempts  int            `json:"cleanup_attempts"`
	LastCleanupError string         `json:"last_cleanup_error,omitempty"`
}

func toStored(r *DiscoveryRecord) storedRecord {
	return storedRecord{
		Identity:         r.Identity,
		Generation:       r.Generation,
		State:            r.State,
		RunnerID:         r.RunnerID,
		Network:          r.Network,
		Address:          r.Address,
		Resources:        append([]string(nil), r.Resources...),
		CreatedAt:        r.CreatedAt,
		CommittedAt:      r.CommittedAt,
		ExpiresAt:        r.ExpiresAt,
		TombstonedAt:     r.TombstonedAt,
		CleanupAttempts:  r.CleanupAttempts,
		LastCleanupError: r.LastCleanupError,
	}
}

func (s storedRecord) toRecord() *DiscoveryRecord {
	return &DiscoveryRecord{
		Identity:         s.Identity,
		Generation:       s.Generation,
		State:            s.State,
		RunnerID:         s.RunnerID,
		Network:          s.Network,
		Address:          s.Address,
		Resources:        append([]string(nil), s.Resources...),
		CreatedAt:        s.CreatedAt,
		CommittedAt:      s.CommittedAt,
		ExpiresAt:        s.ExpiresAt,
		TombstonedAt:     s.TombstonedAt,
		CleanupAttempts:  s.CleanupAttempts,
		LastCleanupError: s.LastCleanupError,
	}
}

// recordPath returns the on-disk path of a record. The filename is derived
// from the identity key and generation with a SHA-256 hash so that arbitrary
// executable paths and versions never produce invalid filenames and two
// generations of the same identity never collide.
func recordPath(dir string, r *DiscoveryRecord) string {
	sum := sha256.Sum256([]byte(r.Identity.key()))
	name := fmt.Sprintf("%s-%d%s", hex.EncodeToString(sum[:16]), r.Generation, recordFileSuffix)
	return filepath.Join(dir, name)
}

// writeRecordAtomic writes the record as JSON and renames it into place, so a
// crash can never leave a partially written record that could be reattached.
func writeRecordAtomic(dir string, r *DiscoveryRecord) error {
	data, err := json.MarshalIndent(toStored(r), "", "  ")
	if err != nil {
		return err
	}

	final := recordPath(dir, r)
	tmp, err := os.CreateTemp(dir, ".discovery-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}

	return os.Rename(tmpName, final)
}

// removeRecord deletes the record file. A missing file is not an error: the
// terminal state is what matters, and a previous attempt may have removed it.
func removeRecord(dir string, r *DiscoveryRecord) error {
	err := os.Remove(recordPath(dir, r))
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// loadRecords reads every well-formed record in dir. Unrecognized files and
// records that fail to decode are skipped rather than failing the whole load:
// a single corrupt file must not make every plugin undiscoverable.
func loadRecords(dir string) ([]*DiscoveryRecord, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var out []*DiscoveryRecord
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), recordFileSuffix) {
			continue
		}

		data, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			continue
		}

		var stored storedRecord
		if err := json.Unmarshal(data, &stored); err != nil {
			continue
		}
		out = append(out, stored.toRecord())
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].Identity.key() != out[j].Identity.key() {
			return out[i].Identity.key() < out[j].Identity.key()
		}
		return out[i].Generation < out[j].Generation
	})
	return out, nil
}
