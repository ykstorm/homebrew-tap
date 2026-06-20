// Package node is the single-node read-through cache: resolve a tag, and on a
// miss ingest the artifact from the origin before serving reassembled bytes.
package node

import (
	"errors"
	"strings"

	"burrow/internal/assemble"
	"burrow/internal/cas"
	"burrow/internal/index"
	"burrow/internal/origin"
)

// ErrBadTag means the tag was not in "name@version" form.
var ErrBadTag = errors.New("node: tag must be name@version")

// Node ties the store, index, and origin into a read-through cache.
type Node struct {
	store *cas.Store
	idx   *index.Index
	orig  origin.Origin
}

// New constructs a Node.
func New(store *cas.Store, idx *index.Index, orig origin.Origin) *Node {
	return &Node{store: store, idx: idx, orig: orig}
}

func splitTag(tag string) (name, version string, ok bool) {
	name, version, ok = strings.Cut(tag, "@")
	if !ok || name == "" || version == "" {
		return "", "", false
	}
	return name, version, true
}

// Ingest fetches name@version from the origin, chunks + stores it, records the
// manifest, and points the tag at it. Returns the manifest hash.
func (n *Node) Ingest(name, version string) (string, error) {
	data, err := n.orig.Fetch(name, version)
	if err != nil {
		return "", err
	}
	m, err := assemble.Build(name, version, data, n.store)
	if err != nil {
		return "", err
	}
	h, err := n.idx.PutManifest(m)
	if err != nil {
		return "", err
	}
	if err := n.idx.SetTag(name+"@"+version, h); err != nil {
		return "", err
	}
	return h, nil
}

// Artifact returns the original bytes for a tag, ingesting from the origin on a
// cache miss. Reassembly verifies every chunk and the total size.
func (n *Node) Artifact(tag string) ([]byte, error) {
	name, version, ok := splitTag(tag)
	if !ok {
		return nil, ErrBadTag
	}
	h, ok := n.idx.Resolve(tag)
	if !ok {
		var err error
		if h, err = n.Ingest(name, version); err != nil {
			return nil, err
		}
	}
	m, err := n.idx.Manifest(h)
	if err != nil {
		return nil, err
	}
	return assemble.Reassemble(m, n.store)
}
