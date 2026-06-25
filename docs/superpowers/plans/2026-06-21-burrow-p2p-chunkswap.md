# Burrow P2P Chunk-Swap (plan #4) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a sidecar fill missing chunks from peer sidecars over HTTP — verifying each chunk by hash — before falling back to origin. This is the BitTorrent-style data path that offloads origin/regional tiers.

**Architecture:** A `peer` package holds a thread-safe peer registry (`Set`) and a `Fetcher` that GETs `/chunk/{hash}` from peers, rejecting any byte string that does not hash to the requested key (peers are untrusted). The node gains an optional `ChunkFetcher`: when reassembling a manifest whose chunks are not all local, it pulls the missing ones from peers and populates its own CAS (read-through fill). The HTTP server exposes `GET /chunk/{hash}` from local CAS so any sidecar is also a peer source. This realizes the design's "manifest is cheap (via raft), chunks move peer-to-peer" split: a node can hold a replicated manifest yet own none of its bytes, and acquire them from neighbors.

**Tech Stack:** Go stdlib (`net/http`, `net/http/httptest`). Membership is injected (static `Set`) for now; gossip auto-discovery via `hashicorp/memberlist` is a deferred follow-up — the `Set` interface is the seam.

**Builds on:** plans #1–#3. Reuses `cas.HashOf` for integrity and the `manifest` chunk list.

**Out of scope (later):** gossip membership (memberlist), parallel multi-source chunk fetch, origin-as-chunk-source backstop, eviction.

---

## File Structure

- Create: `burrow/internal/peer/peer.go` — `Set` + `Fetcher`
- Create: `burrow/internal/peer/peer_test.go`
- Modify: `burrow/internal/node/node.go` — `ChunkFetcher`, `SetPeers`, `GetChunk`, `ensureChunks`, wire into `Artifact`
- Modify: `burrow/internal/httpapi/server.go` — `GET /chunk/{hash}`
- Create: `burrow/internal/httpapi/p2p_test.go` — two-node end-to-end chunk fill

---

## Task 1: Peer registry + fetcher (`peer`)

**Files:** Create `burrow/internal/peer/peer.go`, `burrow/internal/peer/peer_test.go`

- [ ] **Step 1: Failing test**

```go
package peer

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"burrow/internal/cas"
)

func TestFetcherReturnsVerifiedChunk(t *testing.T) {
	body := []byte("chunk-bytes")
	hash := cas.HashOf(body)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/chunk/"+hash {
			w.Write(body)
			return
		}
		http.Error(w, "no", http.StatusNotFound)
	}))
	defer srv.Close()

	f := NewFetcher(NewSet(srv.URL), srv.Client())
	got, ok := f.Fetch(hash)
	if !ok {
		t.Fatal("expected hit")
	}
	if string(got) != string(body) {
		t.Fatalf("got %q", got)
	}
}

func TestFetcherRejectsTamperedChunk(t *testing.T) {
	hash := cas.HashOf([]byte("real"))
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("TAMPERED")) // wrong bytes for the requested hash
	}))
	defer srv.Close()

	f := NewFetcher(NewSet(srv.URL), srv.Client())
	if _, ok := f.Fetch(hash); ok {
		t.Fatal("tampered chunk must be rejected")
	}
}

func TestFetcherMissWhenNoPeerHasIt(t *testing.T) {
	f := NewFetcher(NewSet(), nil)
	if _, ok := f.Fetch(cas.HashOf([]byte("x"))); ok {
		t.Fatal("empty peer set must miss")
	}
}

func TestSetAddRemoveList(t *testing.T) {
	s := NewSet("http://a")
	s.Add("http://b")
	s.Remove("http://a")
	got := s.List()
	if len(got) != 1 || got[0] != "http://b" {
		t.Fatalf("list=%v", got)
	}
}
```

- [ ] **Step 2: Run → fail** — `undefined: NewFetcher`.

- [ ] **Step 3: Implement**

```go
// Package peer provides peer-to-peer chunk exchange between sidecars. Peers are
// untrusted: every fetched chunk is verified against its requested hash.
package peer

import (
	"io"
	"net/http"
	"sort"
	"sync"
	"time"

	"burrow/internal/cas"
)

// Set is a thread-safe registry of peer base URLs.
type Set struct {
	mu    sync.RWMutex
	peers map[string]struct{}
}

// NewSet creates a Set seeded with the given base URLs.
func NewSet(urls ...string) *Set {
	s := &Set{peers: make(map[string]struct{}, len(urls))}
	for _, u := range urls {
		s.peers[u] = struct{}{}
	}
	return s
}

// Add registers a peer base URL.
func (s *Set) Add(url string) {
	s.mu.Lock()
	s.peers[url] = struct{}{}
	s.mu.Unlock()
}

// Remove deregisters a peer base URL.
func (s *Set) Remove(url string) {
	s.mu.Lock()
	delete(s.peers, url)
	s.mu.Unlock()
}

// List returns the peer base URLs in deterministic order.
func (s *Set) List() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.peers))
	for u := range s.peers {
		out = append(out, u)
	}
	sort.Strings(out)
	return out
}

// Fetcher pulls chunks from peers over HTTP.
type Fetcher struct {
	set    *Set
	client *http.Client
}

// NewFetcher builds a Fetcher. A nil client gets a default with a short timeout.
func NewFetcher(set *Set, client *http.Client) *Fetcher {
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	return &Fetcher{set: set, client: client}
}

// Fetch tries each peer's GET /chunk/{hash} in order and returns the first body
// that hashes to hash. A body that does not match is discarded (untrusted peer).
func (f *Fetcher) Fetch(hash string) ([]byte, bool) {
	for _, base := range f.set.List() {
		resp, err := f.client.Get(base + "/chunk/" + hash)
		if err != nil {
			continue
		}
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			continue
		}
		data, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			continue
		}
		if cas.HashOf(data) != hash {
			continue // integrity check failed: reject
		}
		return data, true
	}
	return nil, false
}
```

- [ ] **Step 4: Run → pass** — `go test ./internal/peer/ -v`.
- [ ] **Step 5: Commit** — `git commit -m "feat(burrow): peer registry + integrity-checked chunk fetcher"`

---

## Task 2: Node chunk-fill + chunk serving

**Files:** Modify `burrow/internal/node/node.go`

- [ ] **Step 1: Add the fetcher seam and chunk methods**

Add an interface and an optional field. Append to `node.go`:

```go
// ChunkFetcher pulls a chunk by hash from somewhere else (peers). Implemented
// by peer.Fetcher. Optional: a nil fetcher means "local only".
type ChunkFetcher interface {
	Fetch(hash string) ([]byte, bool)
}

// ErrChunkUnavailable means a manifest chunk was not in the local store and
// could not be fetched from any peer.
var ErrChunkUnavailable = errors.New("node: chunk unavailable from local store or peers")

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
```

Add the field to the struct (modify the existing `Node` definition):

```go
type Node struct {
	store *cas.Store
	idx   Index
	orig  origin.Origin
	peers ChunkFetcher // optional
}
```

- [ ] **Step 2: Wire ensureChunks into Artifact**

In `Artifact`, after loading the manifest and before reassembling:

```go
	m, err := n.idx.Manifest(h)
	if err != nil {
		return nil, err
	}
	if err := n.ensureChunks(m); err != nil {
		return nil, err
	}
	return assemble.Reassemble(m, n.store)
```

- [ ] **Step 3: Run existing node + httpapi tests (no regression)**

Run: `go test ./internal/node/ ./internal/httpapi/ -v`
Expected: PASS — peers is nil, chunks are local after ingest, `ensureChunks` no-ops.

- [ ] **Step 4: Commit** — `git commit -m "feat(burrow): node fills missing chunks from peers"`

---

## Task 3: Serve chunks over HTTP

**Files:** Modify `burrow/internal/httpapi/server.go`

- [ ] **Step 1: Add the route**

Inside `NewServer`, add:

```go
	mux.HandleFunc("GET /chunk/{hash}", func(w http.ResponseWriter, r *http.Request) {
		data, err := n.GetChunk(r.PathValue("hash"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(data)
	})
```

- [ ] **Step 2: Build** — `go build ./...` → exit 0.
- [ ] **Step 3: Commit** — `git commit -m "feat(burrow): serve chunks over HTTP for peers"`

---

## Task 4: Two-node end-to-end chunk fill

**Files:** Create `burrow/internal/httpapi/p2p_test.go`

Node B ingests an artifact (manifest + chunks). Node A is given only the manifest (as raft would replicate it) with an empty origin, and peers pointed at B. A must reconstruct the artifact by pulling chunks from B and filling its own CAS.

- [ ] **Step 1: Test**

```go
package httpapi_test

import (
	"bytes"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"burrow/internal/cas"
	"burrow/internal/chunk"
	"burrow/internal/httpapi"
	"burrow/internal/index"
	"burrow/internal/manifest"
	"burrow/internal/node"
	"burrow/internal/origin"
	"burrow/internal/peer"
)

func TestPeerChunkFillEndToEnd(t *testing.T) {
	// Multi-chunk artifact.
	data := bytes.Repeat([]byte("BURROW"), chunk.Size) // > 1 MiB => several chunks

	// --- Node B: full source ---
	od := t.TempDir()
	if err := os.WriteFile(filepath.Join(od, "sbx-0.33.0.tar.gz"), data, 0o644); err != nil {
		t.Fatal(err)
	}
	bdir := t.TempDir()
	bStore, _ := cas.New(filepath.Join(bdir, "blobs"))
	bIdx, _ := index.New(bStore, filepath.Join(bdir, "tags.json"))
	bNode := node.New(bStore, bIdx, origin.DirOrigin{Dir: od})
	bSrv := httptest.NewServer(httpapi.NewServer(bNode))
	defer bSrv.Close()
	if _, err := bNode.Artifact("sbx@0.33.0"); err != nil { // prime B (ingest)
		t.Fatal(err)
	}

	// --- Node A: manifest only, empty origin, peers -> B ---
	adir := t.TempDir()
	aStore, _ := cas.New(filepath.Join(adir, "blobs"))
	aIdx, _ := index.New(aStore, filepath.Join(adir, "tags.json"))
	aNode := node.New(aStore, aIdx, origin.DirOrigin{Dir: t.TempDir()}) // empty origin

	// Replicate just the manifest into A (chunk hashes, no chunk bytes).
	chunks, _ := chunk.Split(bytes.NewReader(data))
	hashes := make([]string, len(chunks))
	for i, c := range chunks {
		hashes[i] = cas.HashOf(c)
	}
	m := &manifest.Manifest{Name: "sbx", Version: "0.33.0", Size: int64(len(data)), Chunks: hashes}
	mh, err := aIdx.PutManifest(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := aIdx.SetTag("sbx@0.33.0", mh); err != nil {
		t.Fatal(err)
	}

	// Sanity: A lacks the data chunks.
	if aStore.Has(hashes[0]) {
		t.Fatal("precondition failed: A already has chunks")
	}

	aNode.SetPeers(peer.NewFetcher(peer.NewSet(bSrv.URL), bSrv.Client()))

	// A serves by pulling chunks from B.
	got, err := aNode.Artifact("sbx@0.33.0")
	if err != nil {
		t.Fatalf("A.Artifact: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("reconstructed bytes != original")
	}
	// Read-through fill: A now owns the chunks locally.
	for _, h := range hashes {
		if !aStore.Has(h) {
			t.Fatalf("chunk %s not filled into A's store", h)
		}
	}
}
```

- [ ] **Step 2: Run → pass** — `go test ./internal/httpapi/ -run TestPeerChunkFillEndToEnd -v`.
- [ ] **Step 3: Full suite** — `go test ./...`.
- [ ] **Step 4: Commit** — `git commit -m "feat(burrow): two-node end-to-end peer chunk fill test"`

---

## Done criteria

- `go build ./...`, `go vet ./...`, `go test ./...` all pass.
- A node holding only a manifest reconstructs the artifact by fetching chunks from a peer and filling its own store.
- Tampered peer responses are rejected by hash check (`TestFetcherRejectsTamperedChunk`).
- Existing single-node behavior unchanged (peers nil ⇒ `ensureChunks` no-ops).

## Next plan

Plan #5 — predictive prefetch: a warming service that pushes likely-needed manifests + hot chunks to a region's sidecars on task-start/publish, best-effort, never load-bearing.
