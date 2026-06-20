# Burrow Sidecar (single node) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wrap the Burrow core (plan #1) into a runnable single-node sidecar: on a cache miss it fetches an artifact from an origin, chunks + stores it, and serves the reassembled bytes over a Homebrew-compatible HTTP endpoint. Plus a 1-line Ruby cask shim pointing `brew` at the sidecar.

**Architecture:** Four new Go packages on top of plan #1. `origin` abstracts where raw artifacts come from (a local directory for v1, HTTP later) behind a one-method interface. `index` maps a tag (`name@version`) to a manifest hash and stores/loads manifests via the existing CAS, persisting the tag table as JSON. `node` is the read-through cache: resolve tag → if miss, ingest from origin (Build) → reassemble and return bytes. `httpapi` exposes `GET /artifact/{name}/{version}` returning the original tarball bytes, which is exactly what a cask `url` GETs. A `cmd/sidecar` wires it together. The Ruby cask only changes its `url` to the sidecar; `sha256` is unchanged because reassembled bytes are byte-identical to the upstream tarball.

**Tech Stack:** Go stdlib only (`net/http` with 1.22 method+wildcard routing, `net/http/httptest`, `os`, `sync`, `encoding/json`). Ruby for the cask shim (Homebrew DSL). No third-party deps.

**Depends on:** plan #1 packages `burrow/internal/{cas,chunk,manifest,assemble}`.

**Out of scope (later plans):** Raft (#3), P2P gossip (#4), prefetch (#5), control plane (#6), HTTP origin from real GitHub (trivial follow-up to `origin`).

---

## File Structure

- Create: `burrow/internal/origin/origin.go` — `Origin` interface + `DirOrigin`
- Create: `burrow/internal/origin/origin_test.go`
- Create: `burrow/internal/index/index.go` — tag table + manifest store/load
- Create: `burrow/internal/index/index_test.go`
- Create: `burrow/internal/node/node.go` — read-through cache (Ingest, Artifact)
- Create: `burrow/internal/node/node_test.go`
- Create: `burrow/internal/httpapi/server.go` — HTTP handlers
- Create: `burrow/internal/httpapi/server_test.go` — httptest
- Create: `burrow/cmd/sidecar/main.go` — CLI wiring
- Create: `burrow/examples/cask/sbx-burrow.rb` — Ruby cask shim example

---

## Task 1: Origin (`origin`)

**Files:** Create `burrow/internal/origin/origin.go`, `burrow/internal/origin/origin_test.go`

- [ ] **Step 1: Failing test**

```go
package origin

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDirOriginFetch(t *testing.T) {
	dir := t.TempDir()
	want := []byte("fake-tarball-bytes")
	if err := os.WriteFile(filepath.Join(dir, "sbx-0.33.0.tar.gz"), want, 0o644); err != nil {
		t.Fatal(err)
	}
	o := DirOrigin{Dir: dir}
	got, err := o.Fetch("sbx", "0.33.0")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestDirOriginMissing(t *testing.T) {
	o := DirOrigin{Dir: t.TempDir()}
	if _, err := o.Fetch("sbx", "9.9.9"); err != ErrNotFound {
		t.Fatalf("got %v want ErrNotFound", err)
	}
}
```

- [ ] **Step 2: Run → fail** — `go test ./internal/origin/ -v` → `undefined: DirOrigin`.

- [ ] **Step 3: Implement**

```go
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
```

- [ ] **Step 4: Run → pass** — `go test ./internal/origin/ -v`.
- [ ] **Step 5: Commit** — `git commit -m "feat(burrow): origin interface + DirOrigin"`

---

## Task 2: Index (`index`)

**Files:** Create `burrow/internal/index/index.go`, `burrow/internal/index/index_test.go`

- [ ] **Step 1: Failing test**

```go
package index

import (
	"path/filepath"
	"testing"

	"burrow/internal/cas"
	"burrow/internal/manifest"
)

func newIdx(t *testing.T) *Index {
	t.Helper()
	dir := t.TempDir()
	s, _ := cas.New(filepath.Join(dir, "blobs"))
	i, err := New(s, filepath.Join(dir, "tags.json"))
	if err != nil {
		t.Fatal(err)
	}
	return i
}

func TestPutAndGetManifest(t *testing.T) {
	i := newIdx(t)
	m := &manifest.Manifest{Name: "sbx", Version: "0.33.0", Size: 3, Chunks: []string{"aa"}}
	h, err := i.PutManifest(m)
	if err != nil {
		t.Fatal(err)
	}
	want, _ := m.Hash()
	if h != want {
		t.Fatalf("manifest hash %s want %s", h, want)
	}
	got, err := i.Manifest(h)
	if err != nil {
		t.Fatal(err)
	}
	if got.Version != "0.33.0" {
		t.Fatalf("roundtrip version %s", got.Version)
	}
}

func TestSetResolveTag(t *testing.T) {
	i := newIdx(t)
	if err := i.SetTag("sbx@0.33.0", "abc123"); err != nil {
		t.Fatal(err)
	}
	h, ok := i.Resolve("sbx@0.33.0")
	if !ok || h != "abc123" {
		t.Fatalf("resolve got %q,%v", h, ok)
	}
	if _, ok := i.Resolve("nope@0"); ok {
		t.Fatal("resolved unknown tag")
	}
}

func TestTagsPersistAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	s, _ := cas.New(filepath.Join(dir, "blobs"))
	tags := filepath.Join(dir, "tags.json")
	i1, _ := New(s, tags)
	i1.SetTag("sbx@0.33.0", "deadbeef")
	i2, err := New(s, tags) // reopen
	if err != nil {
		t.Fatal(err)
	}
	if h, ok := i2.Resolve("sbx@0.33.0"); !ok || h != "deadbeef" {
		t.Fatalf("persisted resolve got %q,%v", h, ok)
	}
}
```

- [ ] **Step 2: Run → fail** — `undefined: New`.

- [ ] **Step 3: Implement**

```go
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
```

- [ ] **Step 4: Run → pass** — `go test ./internal/index/ -v`.
- [ ] **Step 5: Commit** — `git commit -m "feat(burrow): metadata index (tags + manifest store)"`

---

## Task 3: Node (`node`) — read-through cache

**Files:** Create `burrow/internal/node/node.go`, `burrow/internal/node/node_test.go`

- [ ] **Step 1: Failing test**

```go
package node

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"burrow/internal/cas"
	"burrow/internal/index"
	"burrow/internal/origin"
)

func newNode(t *testing.T, originDir string) *Node {
	t.Helper()
	dir := t.TempDir()
	s, _ := cas.New(filepath.Join(dir, "blobs"))
	idx, _ := index.New(s, filepath.Join(dir, "tags.json"))
	return New(s, idx, origin.DirOrigin{Dir: originDir})
}

func TestArtifactReadThroughThenCached(t *testing.T) {
	od := t.TempDir()
	want := bytes.Repeat([]byte("Q"), 1500)
	os.WriteFile(filepath.Join(od, "sbx-0.33.0.tar.gz"), want, 0o644)
	n := newNode(t, od)

	// First call: miss -> ingest from origin -> serve.
	got, err := n.Artifact("sbx@0.33.0")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("read-through bytes != origin")
	}

	// Delete the origin file; a cached hit must still serve.
	os.Remove(filepath.Join(od, "sbx-0.33.0.tar.gz"))
	got2, err := n.Artifact("sbx@0.33.0")
	if err != nil {
		t.Fatalf("cached hit failed: %v", err)
	}
	if !bytes.Equal(got2, want) {
		t.Fatal("cached bytes != origin")
	}
}

func TestArtifactBadTag(t *testing.T) {
	n := newNode(t, t.TempDir())
	if _, err := n.Artifact("no-at-sign"); err != ErrBadTag {
		t.Fatalf("got %v want ErrBadTag", err)
	}
}

func TestArtifactMissingAtOrigin(t *testing.T) {
	n := newNode(t, t.TempDir())
	if _, err := n.Artifact("sbx@9.9.9"); err == nil {
		t.Fatal("expected error for missing origin artifact")
	}
}
```

- [ ] **Step 2: Run → fail** — `undefined: New`.

- [ ] **Step 3: Implement**

```go
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
```

- [ ] **Step 4: Run → pass** — `go test ./internal/node/ -v`.
- [ ] **Step 5: Commit** — `git commit -m "feat(burrow): read-through cache node"`

---

## Task 4: HTTP API (`httpapi`)

**Files:** Create `burrow/internal/httpapi/server.go`, `burrow/internal/httpapi/server_test.go`

- [ ] **Step 1: Failing test**

```go
package httpapi

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"burrow/internal/cas"
	"burrow/internal/index"
	"burrow/internal/node"
	"burrow/internal/origin"
)

func newSrv(t *testing.T, originDir string) http.Handler {
	t.Helper()
	dir := t.TempDir()
	s, _ := cas.New(filepath.Join(dir, "blobs"))
	idx, _ := index.New(s, filepath.Join(dir, "tags.json"))
	n := node.New(s, idx, origin.DirOrigin{Dir: originDir})
	return NewServer(n)
}

func TestServeArtifact(t *testing.T) {
	od := t.TempDir()
	want := []byte("real-tarball")
	os.WriteFile(filepath.Join(od, "sbx-0.33.0.tar.gz"), want, 0o644)
	srv := httptest.NewServer(newSrv(t, od))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/artifact/sbx/0.33.0")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	got, _ := io.ReadAll(resp.Body)
	if string(got) != string(want) {
		t.Fatalf("body %q want %q", got, want)
	}
}

func TestServeArtifactMissing(t *testing.T) {
	srv := httptest.NewServer(newSrv(t, t.TempDir()))
	defer srv.Close()
	resp, _ := http.Get(srv.URL + "/artifact/sbx/9.9.9")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status %d want 404", resp.StatusCode)
	}
}

func TestHealthz(t *testing.T) {
	srv := httptest.NewServer(newSrv(t, t.TempDir()))
	defer srv.Close()
	resp, _ := http.Get(srv.URL + "/healthz")
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
}
```

- [ ] **Step 2: Run → fail** — `undefined: NewServer`.

- [ ] **Step 3: Implement**

```go
// Package httpapi serves Burrow artifacts over HTTP. The /artifact route is
// shaped so a Homebrew cask url can GET it directly and verify the sha256.
package httpapi

import (
	"errors"
	"net/http"

	"burrow/internal/node"
	"burrow/internal/origin"
)

// NewServer returns an http.Handler backed by the given node.
func NewServer(n *node.Node) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("GET /artifact/{name}/{version}", func(w http.ResponseWriter, r *http.Request) {
		tag := r.PathValue("name") + "@" + r.PathValue("version")
		data, err := n.Artifact(tag)
		if err != nil {
			status := http.StatusInternalServerError
			if errors.Is(err, origin.ErrNotFound) || errors.Is(err, node.ErrBadTag) {
				status = http.StatusNotFound
			}
			http.Error(w, err.Error(), status)
			return
		}
		w.Header().Set("Content-Type", "application/gzip")
		_, _ = w.Write(data)
	})
	return mux
}
```

- [ ] **Step 4: Run → pass** — `go test ./internal/httpapi/ -v`.
- [ ] **Step 5: Commit** — `git commit -m "feat(burrow): Homebrew-compatible HTTP artifact endpoint"`

---

## Task 5: CLI (`cmd/sidecar`)

**Files:** Create `burrow/cmd/sidecar/main.go`

- [ ] **Step 1: Implement**

```go
// Command sidecar runs a single-node Burrow cache serving artifacts over HTTP.
package main

import (
	"flag"
	"log"
	"net/http"
	"path/filepath"

	"burrow/internal/cas"
	"burrow/internal/httpapi"
	"burrow/internal/index"
	"burrow/internal/node"
	"burrow/internal/origin"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:7777", "listen address")
	cacheDir := flag.String("cache", "burrow-cache", "local cache directory")
	originDir := flag.String("origin", "origin", "directory of {name}-{version}.tar.gz artifacts")
	flag.Parse()

	store, err := cas.New(filepath.Join(*cacheDir, "blobs"))
	if err != nil {
		log.Fatalf("cas: %v", err)
	}
	idx, err := index.New(store, filepath.Join(*cacheDir, "tags.json"))
	if err != nil {
		log.Fatalf("index: %v", err)
	}
	n := node.New(store, idx, origin.DirOrigin{Dir: *originDir})

	log.Printf("burrow sidecar listening on %s (cache=%s origin=%s)", *addr, *cacheDir, *originDir)
	if err := http.ListenAndServe(*addr, httpapi.NewServer(n)); err != nil {
		log.Fatal(err)
	}
}
```

- [ ] **Step 2: Verify build** — `go build ./...` → exit 0.
- [ ] **Step 3: Commit** — `git commit -m "feat(burrow): sidecar CLI entrypoint"`

---

## Task 6: Ruby cask shim (example)

**Files:** Create `burrow/examples/cask/sbx-burrow.rb`

Note: this is an EXAMPLE under `burrow/examples/`, NOT a change to the live `Casks/`. Pointing the real cask at localhost would break normal installs. The only diff from the upstream cask is the `url` line; `sha256` is unchanged because the sidecar returns byte-identical bytes.

- [ ] **Step 1: Create the file**

```ruby
# Example cask: identical to the upstream sbx cask except `url` points at a
# local Burrow sidecar (GET /artifact/{name}/{version}) instead of GitHub.
# The sidecar returns byte-identical bytes, so `sha256` is unchanged and
# `brew`'s integrity check still passes.
cask "sbx-burrow" do
  version "0.33.0"
  sha256 "72b6347eca940cd8998084ed1f409d28c7d742064efb0d7f8baf07395a2a6eb7"

  url "http://127.0.0.1:7777/artifact/sbx/#{version}"
  name "Docker Sandboxes (via Burrow)"
  desc "Build, run, and govern agents across the software development lifecycle"
  homepage "https://github.com/docker/sbx-releases"

  depends_on arch:  :arm64,
             macos: :sonoma

  binary "bin/sbx", target: "sbx"
end
```

- [ ] **Step 2: Commit** — `git commit -m "docs(burrow): example Ruby cask shim pointing at sidecar"`

---

## Done criteria

- `go build ./...` and `go test ./...` pass from `burrow/`.
- A running `sidecar` serves `GET /artifact/sbx/0.33.0` returning bytes whose sha256 matches the upstream cask's `sha256`.
- Read-through proven by `TestArtifactReadThroughThenCached` (serves after origin file deleted).
- Ruby cask shim differs from upstream by one `url` line only.

## Next plan

Plan #3 — regional tier + Raft metadata: replace the single-node JSON tag table in `index` with a Raft-replicated table for strong, multi-node `latest` resolution, and add the regional cache tier between sidecar and origin.
