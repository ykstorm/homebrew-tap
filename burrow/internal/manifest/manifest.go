// Package manifest describes an artifact as an ordered list of chunk hashes.
package manifest

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

// Manifest maps a named artifact version to its ordered chunk hashes.
type Manifest struct {
	Name    string   `json:"name"`
	Version string   `json:"version"`
	Size    int64    `json:"size"`
	Chunks  []string `json:"chunks"`
}

// Marshal returns the manifest's JSON encoding. Go's encoding/json emits struct
// fields in declaration order, so this encoding is stable for a given value.
func (m *Manifest) Marshal() ([]byte, error) {
	return json.Marshal(m)
}

// Unmarshal parses a manifest from JSON.
func Unmarshal(data []byte) (*Manifest, error) {
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

// Hash returns the sha256 hex digest of the manifest's JSON encoding. This is
// the manifest-hash the control plane maps a tag to.
func (m *Manifest) Hash() (string, error) {
	data, err := m.Marshal()
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}
