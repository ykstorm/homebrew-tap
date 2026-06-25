// Package cas is a content-addressed blob store keyed by sha256 hex digests.
package cas

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"time"
)

var (
	// ErrNotFound is returned by Get when the blob is absent.
	ErrNotFound = errors.New("cas: blob not found")
	// ErrCorrupt is returned by Get when stored bytes do not match the key.
	ErrCorrupt = errors.New("cas: stored blob hash mismatch")
)

// Store is a content-addressed blob store backed by a directory.
type Store struct {
	root string
}

// New creates a Store rooted at dir, creating it if needed.
func New(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &Store{root: dir}, nil
}

// HashOf returns the sha256 hex digest of data.
func HashOf(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func (s *Store) path(hash string) string { return filepath.Join(s.root, hash) }

// Has reports whether a blob with the given hash is stored.
func (s *Store) Has(hash string) bool {
	_, err := os.Stat(s.path(hash))
	return err == nil
}

// Put stores data and returns its hash. It is idempotent: storing identical
// bytes twice yields the same hash and no duplicate file. The write is atomic
// (temp file + rename) so a crash never leaves a partial blob under its key.
func (s *Store) Put(data []byte) (string, error) {
	hash := HashOf(data)
	p := s.path(hash)
	if _, err := os.Stat(p); err == nil {
		return hash, nil
	}
	tmp, err := os.CreateTemp(s.root, ".tmp-*")
	if err != nil {
		return "", err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return "", err
	}
	if err := tmp.Close(); err != nil {
		return "", err
	}
	if err := os.Rename(tmp.Name(), p); err != nil {
		return "", err
	}
	return hash, nil
}

// Root returns the store's backing directory. Useful for tooling and tests.
func (s *Store) Root() string { return s.root }

// Entry describes a stored blob.
type Entry struct {
	Hash    string
	Size    int64
	ModTime time.Time
}

// List returns one Entry per stored blob. Temporary files from in-flight or
// crashed Puts (".tmp-*") are skipped.
func (s *Store) List() ([]Entry, error) {
	des, err := os.ReadDir(s.root)
	if err != nil {
		return nil, err
	}
	out := make([]Entry, 0, len(des))
	for _, de := range des {
		if de.IsDir() || strings.HasPrefix(de.Name(), ".tmp") {
			continue
		}
		info, err := de.Info()
		if err != nil {
			continue
		}
		out = append(out, Entry{Hash: de.Name(), Size: info.Size(), ModTime: info.ModTime()})
	}
	return out, nil
}

// Delete removes a blob. Removing an absent blob is not an error.
func (s *Store) Delete(hash string) error {
	err := os.Remove(s.path(hash))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

// Get returns the blob for hash, verifying its integrity. It returns
// ErrNotFound if absent and ErrCorrupt if the stored bytes do not hash to key.
func (s *Store) Get(hash string) ([]byte, error) {
	data, err := os.ReadFile(s.path(hash))
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if HashOf(data) != hash {
		return nil, ErrCorrupt
	}
	return data, nil
}
