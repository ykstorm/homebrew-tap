// Package assemble bridges chunk, cas, and manifest: Build turns bytes into
// stored chunks plus a manifest; Reassemble rebuilds bytes from a manifest,
// verifying integrity end to end.
package assemble

import (
	"bytes"
	"errors"

	"burrow/internal/cas"
	"burrow/internal/chunk"
	"burrow/internal/manifest"
)

// ErrSizeMismatch is returned when reassembled bytes do not match the size
// declared in the manifest.
var ErrSizeMismatch = errors.New("assemble: reassembled size mismatch")

// Build chunks data, stores every chunk in store, and returns a manifest
// referencing them in order. Identical chunks dedup automatically in the store.
func Build(name, version string, data []byte, store *cas.Store) (*manifest.Manifest, error) {
	chunks, err := chunk.Split(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	hashes := make([]string, 0, len(chunks))
	for _, c := range chunks {
		h, err := store.Put(c)
		if err != nil {
			return nil, err
		}
		hashes = append(hashes, h)
	}
	return &manifest.Manifest{
		Name:    name,
		Version: version,
		Size:    int64(len(data)),
		Chunks:  hashes,
	}, nil
}

// Reassemble fetches each chunk named by m from store (cas.Get verifies each
// chunk's hash), concatenates them, and checks the total against m.Size.
func Reassemble(m *manifest.Manifest, store *cas.Store) ([]byte, error) {
	var buf bytes.Buffer
	for _, h := range m.Chunks {
		c, err := store.Get(h)
		if err != nil {
			return nil, err
		}
		buf.Write(c)
	}
	out := buf.Bytes()
	if int64(len(out)) != m.Size {
		return nil, ErrSizeMismatch
	}
	return out, nil
}
