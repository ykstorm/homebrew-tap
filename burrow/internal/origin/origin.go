// Package origin abstracts where raw (unchunked) artifacts come from.
package origin

import (
	"errors"
	"os"
	"path/filepath"
)

// ErrNotFound means the origin has no such artifact.
var ErrNotFound = errors.New("origin: artifact not found")

// Origin fetches the raw bytes of a named artifact version.
type Origin interface {
	Fetch(name, version string) ([]byte, error)
}

// DirOrigin reads artifacts from a local directory, expecting files named
// "{name}-{version}.tar.gz". Useful for tests and air-gapped mirrors.
type DirOrigin struct{ Dir string }

// Fetch reads {Dir}/{name}-{version}.tar.gz.
func (d DirOrigin) Fetch(name, version string) ([]byte, error) {
	p := filepath.Join(d.Dir, name+"-"+version+".tar.gz")
	data, err := os.ReadFile(p)
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNotFound
	}
	return data, err
}
