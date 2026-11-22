// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

package plugin

import (
	"crypto/sha256"
	"encoding/hex"
	"hash"
	"io"
	"os"
)

// digestHashes maps the digest algorithm names accepted by NewPluginIdentity to
// their hash constructors. The set is intentionally small: discovery records
// only need the standard, collision-resistant algorithms used for executable
// integrity.
var digestHashes = map[string]func() hash.Hash{
	"sha256": sha256.New,
}

// hashFile returns the hex-encoded digest of the file at path using h.
func hashFile(path string, h hash.Hash) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()

	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
