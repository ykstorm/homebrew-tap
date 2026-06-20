// Package index maps tags (name@version) to manifest hashes and stores
// manifests in the content-addressed store. The tag table is persisted as JSON.
package index

import (
	"encoding/json"
	"os"
	"sync"

	"burrow/internal/cas"
	"burrow/internal/manifest"
)

// Index is the single-node metadata layer. In later plans the tag table moves
// behind Raft for strong consistency across regions; the method set stays.
type Index struct {
	store    *cas.Store
	tagsPath string
	mu       sync.Mutex
	tags     map[string]string // tag -> manifest hash
}

// New opens an Index, loading the persisted tag table if present.
func New(store *cas.Store, tagsPath string) (*Index, error) {
	i := &Index{store: store, tagsPath: tagsPath, tags: map[string]string{}}
	data, err := os.ReadFile(tagsPath)
	if os.IsNotExist(err) {
		return i, nil
	}
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(data, &i.tags); err != nil {
		return nil, err
	}
	return i, nil
}

// PutManifest stores the manifest's JSON in the CAS. The returned key equals
// manifest.Hash() because both are sha256 over the same canonical JSON.
func (i *Index) PutManifest(m *manifest.Manifest) (string, error) {
	data, err := m.Marshal()
	if err != nil {
		return "", err
	}
	return i.store.Put(data)
}

// Manifest loads and parses the manifest stored under hash.
func (i *Index) Manifest(hash string) (*manifest.Manifest, error) {
	data, err := i.store.Get(hash)
	if err != nil {
		return nil, err
	}
	return manifest.Unmarshal(data)
}

// SetTag points tag at a manifest hash and persists the tag table atomically.
func (i *Index) SetTag(tag, manifestHash string) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.tags[tag] = manifestHash
	return i.save()
}

// Resolve returns the manifest hash for tag.
func (i *Index) Resolve(tag string) (string, bool) {
	i.mu.Lock()
	defer i.mu.Unlock()
	h, ok := i.tags[tag]
	return h, ok
}

func (i *Index) save() error {
	data, err := json.Marshal(i.tags)
	if err != nil {
		return err
	}
	tmp := i.tagsPath + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, i.tagsPath)
}
