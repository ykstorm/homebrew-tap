// Package node is the single-node read-through cache: resolve a tag, and on a
// miss ingest the artifact from the origin before serving reassembled bytes.
package node

import (
	"errors"
	"strings"

	"burrow/internal/assemble"
	"burrow/internal/cas"
	"burrow/internal/manifest"
	"burrow/internal/origin"
)

// ErrBadTag means the tag was not in "name@version" form.
var ErrBadTag = errors.New("node: tag must be name@version")

// TagStore is the strongly-consistent tag → manifest-hash mapping. In a single
// node this is the JSON index; in a cluster it is raftindex.
type TagStore interface {
	Resolve(tag string) (string, bool)
	SetTag(tag, manifestHash string) error
}

// ManifestStore persists and retrieves manifests (content-addressed, so no
// consensus is needed on them).
type ManifestStore interface {
	PutManifest(*manifest.Manifest) (string, error)
	Manifest(hash string) (*manifest.Manifest, error)
}

// Index combines both backends. *index.Index satisfies it directly; so does a
// pairing of a raftindex.Store (tags) with an index.Index (manifests).
type Index interface {
	TagStore
	ManifestStore
}

// ChunkFetcher pulls a chunk by hash from somewhere else (peers). Implemented
// by peer.Fetcher. Optional: a nil fetcher means "local only".
type ChunkFetcher interface {
	Fetch(hash string) ([]byte, bool)
}

// ErrChunkUnavailable means a manifest chunk was not in the local store and
// could not be fetched from any peer.
var ErrChunkUnavailable = errors.New("node: chunk unavailable from local store or peers")

// Node ties the store, index, and origin into a read-through cache.
type Node struct {
	store *cas.Store
	idx   Index
	orig  origin.Origin
	peers ChunkFetcher // optional
}

// New constructs a Node.
func New(store *cas.Store, idx Index, orig origin.Origin) *Node {
	return &Node{store: store, idx: idx, orig: orig}
}

// SetPeers attaches a peer chunk fetcher used to fill cache misses.
func (n *Node) SetPeers(f ChunkFetcher) { n.peers = f }

// GetChunk returns a locally-stored chunk (verified by the CAS). Used by the
// HTTP /chunk endpoint so this node can serve peers.
func (n *Node) GetChunk(hash string) ([]byte, error) { return n.store.Get(hash) }

// ensureChunks guarantees every chunk in m is present locally, pulling missing
// ones from peers and populating the local store (read-through fill).
func (n *Node) ensureChunks(m *manifest.Manifest) error {
	for _, h := range m.Chunks {
		if n.store.Has(h) {
			continue
		}
		if n.peers != nil {
			if data, ok := n.peers.Fetch(h); ok {
				if _, err := n.store.Put(data); err != nil {
					return err
				}
				continue
			}
		}
		return ErrChunkUnavailable
	}
	return nil
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
	if err := n.ensureChunks(m); err != nil {
		return nil, err
	}
	return assemble.Reassemble(m, n.store)
}
