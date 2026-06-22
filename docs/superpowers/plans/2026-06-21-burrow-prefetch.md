# Burrow Predictive Prefetch (plan #5) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Hide latency by pre-warming a sidecar's cache with artifacts an agent task is likely to need next, learned from co-access history. Strictly best-effort: warming never blocks or changes correctness.

**Architecture:** A `prefetch` package with two pieces. `Model` is a heuristic co-occurrence learner: it records which tags get used together within a session and predicts the top-k tags most often co-accessed with a seed. `Warmer` takes those predictions and asks a `Prefetcher` (the node) to pull each predicted artifact's chunks into the local store ahead of demand — swallowing failures so warming is never load-bearing. The node gains `Prefetch(tag)`: resolve the manifest and ensure its chunks are local (ingest from origin if the tag is unknown, else fill from peers) WITHOUT reassembling/returning bytes — a cheap cache-fill, not a serve.

**Tech Stack:** Go stdlib (`sync`, `sort`). Builds on #1–#4 (reuses node `ensureChunks`/`Ingest`, peer fill).

**Design fidelity:** This is layer 2 of the three agentic layers — heuristic first (co-occurrence/recency), ML later behind the same `Model.Predict` seam; best-effort, never correctness-critical.

**Out of scope (later):** recency/time-decay weighting, ML model, publish-triggered fan-out push to many sidecars, eviction interplay.

---

## File Structure

- Modify: `burrow/internal/node/node.go` — add `Prefetch(tag)`
- Create: `burrow/internal/prefetch/model.go` — co-occurrence learner
- Create: `burrow/internal/prefetch/model_test.go`
- Create: `burrow/internal/prefetch/warmer.go` — `Prefetcher` + `Warmer`
- Create: `burrow/internal/prefetch/warmer_test.go`
- Create: `burrow/internal/httpapi/prefetch_test.go` — warmer drives real node to pull from a peer

---

## Task 1: Node.Prefetch (cheap cache-fill)

**Files:** Modify `burrow/internal/node/node.go`

- [ ] **Step 1: Failing test** — append to `burrow/internal/node/node_test.go`

```go
func TestPrefetchFillsChunksWithoutServing(t *testing.T) {
	od := t.TempDir()
	want := bytes.Repeat([]byte("P"), 1500)
	os.WriteFile(filepath.Join(od, "sbx-0.33.0.tar.gz"), want, 0o644)
	n := newNode(t, od)

	if err := n.Prefetch("sbx@0.33.0"); err != nil {
		t.Fatalf("Prefetch: %v", err)
	}
	// Tag now resolves and a subsequent Artifact serves from cache even if the
	// origin disappears -- proving Prefetch warmed the store.
	os.Remove(filepath.Join(od, "sbx-0.33.0.tar.gz"))
	got, err := n.Artifact("sbx@0.33.0")
	if err != nil {
		t.Fatalf("Artifact after prefetch: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("served bytes != original")
	}
}

func TestPrefetchBadTag(t *testing.T) {
	n := newNode(t, t.TempDir())
	if err := n.Prefetch("no-at"); err != ErrBadTag {
		t.Fatalf("got %v want ErrBadTag", err)
	}
}
```

- [ ] **Step 2: Run → fail** — `n.Prefetch undefined`.

- [ ] **Step 3: Implement** — append to `node.go`

```go
// Prefetch warms the local cache for a tag without serving bytes: it resolves
// the manifest (ingesting from origin if the tag is unknown) and ensures every
// chunk is present locally (filling from peers on a miss). Cheaper than
// Artifact because it skips reassembly.
func (n *Node) Prefetch(tag string) error {
	name, version, ok := splitTag(tag)
	if !ok {
		return ErrBadTag
	}
	h, ok := n.idx.Resolve(tag)
	if !ok {
		// Unknown tag: ingest from origin (this already stores all chunks).
		_, err := n.Ingest(name, version)
		return err
	}
	m, err := n.idx.Manifest(h)
	if err != nil {
		return err
	}
	return n.ensureChunks(m)
}
```

- [ ] **Step 4: Run → pass** — `go test ./internal/node/ -run TestPrefetch -v`.
- [ ] **Step 5: Commit** — `git commit -m "feat(burrow): node.Prefetch cache-fill"`

---

## Task 2: Co-occurrence model

**Files:** Create `burrow/internal/prefetch/model.go`, `burrow/internal/prefetch/model_test.go`

- [ ] **Step 1: Failing test**

```go
package prefetch

import (
	"reflect"
	"testing"
)

func TestPredictRanksByCoAccess(t *testing.T) {
	m := NewModel()
	// Session 1 and 2: A with B. Session 3: A with C.
	m.Observe("s1", "A")
	m.Observe("s1", "B")
	m.Observe("s2", "A")
	m.Observe("s2", "B")
	m.Observe("s3", "A")
	m.Observe("s3", "C")

	got := m.Predict("A", 2)
	if !reflect.DeepEqual(got, []string{"B", "C"}) {
		t.Fatalf("predict=%v want [B C]", got)
	}
}

func TestPredictExcludesSeedAndUnknown(t *testing.T) {
	m := NewModel()
	m.Observe("s1", "A")
	m.Observe("s1", "B")
	if got := m.Predict("A", 5); !reflect.DeepEqual(got, []string{"B"}) {
		t.Fatalf("predict=%v want [B]", got)
	}
	if got := m.Predict("Z", 5); len(got) != 0 {
		t.Fatalf("predict unknown=%v want empty", got)
	}
}

func TestObserveIsIdempotentWithinSession(t *testing.T) {
	m := NewModel()
	m.Observe("s1", "A")
	m.Observe("s1", "B")
	m.Observe("s1", "B") // repeat must not double-count
	m.Observe("s2", "A")
	m.Observe("s2", "C")
	// A-B counted once, A-C counted once => tie, broken by name asc.
	if got := m.Predict("A", 2); !reflect.DeepEqual(got, []string{"B", "C"}) {
		t.Fatalf("predict=%v want [B C]", got)
	}
}
```

- [ ] **Step 2: Run → fail** — `undefined: NewModel`.

- [ ] **Step 3: Implement**

```go
// Package prefetch predicts and pre-warms artifacts an agent task is likely to
// need next. Heuristic now (co-occurrence); ML later behind Model.Predict.
package prefetch

import (
	"sort"
	"sync"
)

// Model is a co-occurrence learner: tags used together in a session reinforce
// each other, and Predict returns the tags most associated with a seed.
type Model struct {
	mu       sync.Mutex
	sessions map[string]map[string]bool // session -> set of tags seen
	pairs    map[string]map[string]int  // tag -> co-tag -> count
}

// NewModel creates an empty Model.
func NewModel() *Model {
	return &Model{
		sessions: map[string]map[string]bool{},
		pairs:    map[string]map[string]int{},
	}
}

// Observe records that tag was used within session. Repeats within the same
// session do not double-count.
func (m *Model) Observe(session, tag string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	set := m.sessions[session]
	if set == nil {
		set = map[string]bool{}
		m.sessions[session] = set
	}
	if set[tag] {
		return
	}
	for other := range set {
		m.inc(tag, other)
		m.inc(other, tag)
	}
	set[tag] = true
}

func (m *Model) inc(a, b string) {
	row := m.pairs[a]
	if row == nil {
		row = map[string]int{}
		m.pairs[a] = row
	}
	row[b]++
}

// Predict returns up to k tags most co-accessed with seed, excluding seed,
// ranked by count desc then name asc (deterministic).
func (m *Model) Predict(seed string, k int) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	row := m.pairs[seed]
	if len(row) == 0 {
		return nil
	}
	type pc struct {
		tag   string
		count int
	}
	list := make([]pc, 0, len(row))
	for tag, c := range row {
		list = append(list, pc{tag, c})
	}
	sort.Slice(list, func(i, j int) bool {
		if list[i].count != list[j].count {
			return list[i].count > list[j].count
		}
		return list[i].tag < list[j].tag
	})
	if k > len(list) {
		k = len(list)
	}
	out := make([]string, k)
	for i := 0; i < k; i++ {
		out[i] = list[i].tag
	}
	return out
}
```

- [ ] **Step 4: Run → pass** — `go test ./internal/prefetch/ -run TestPredict -v && go test ./internal/prefetch/ -run TestObserve -v`.
- [ ] **Step 5: Commit** — `git commit -m "feat(burrow): co-occurrence prefetch model"`

---

## Task 3: Warmer

**Files:** Create `burrow/internal/prefetch/warmer.go`, `burrow/internal/prefetch/warmer_test.go`

- [ ] **Step 1: Failing test**

```go
package prefetch

import (
	"errors"
	"reflect"
	"testing"
)

type fakePrefetcher struct {
	warmed []string
	fail   map[string]bool
}

func (f *fakePrefetcher) Prefetch(tag string) error {
	if f.fail[tag] {
		return errors.New("boom")
	}
	f.warmed = append(f.warmed, tag)
	return nil
}

func TestWarmerWarmsPredictions(t *testing.T) {
	m := NewModel()
	m.Observe("s1", "A")
	m.Observe("s1", "B")
	m.Observe("s2", "A")
	m.Observe("s2", "B")
	m.Observe("s3", "A")
	m.Observe("s3", "C")

	fp := &fakePrefetcher{}
	w := NewWarmer(fp, m, 2)
	warmed := w.WarmFor("A")
	if !reflect.DeepEqual(warmed, []string{"B", "C"}) {
		t.Fatalf("warmed=%v want [B C]", warmed)
	}
	if !reflect.DeepEqual(fp.warmed, []string{"B", "C"}) {
		t.Fatalf("prefetcher saw %v", fp.warmed)
	}
}

func TestWarmerIsBestEffort(t *testing.T) {
	m := NewModel()
	m.Observe("s1", "A")
	m.Observe("s1", "B")
	m.Observe("s1", "C")

	fp := &fakePrefetcher{fail: map[string]bool{"B": true}}
	w := NewWarmer(fp, m, 5)
	warmed := w.WarmFor("A") // B fails, C still warms
	for _, tag := range warmed {
		if tag == "B" {
			t.Fatal("failed prefetch must not be reported warmed")
		}
	}
	found := false
	for _, tag := range warmed {
		if tag == "C" {
			found = true
		}
	}
	if !found {
		t.Fatal("a failure must not stop warming the rest")
	}
}
```

- [ ] **Step 2: Run → fail** — `undefined: NewWarmer`.

- [ ] **Step 3: Implement**

```go
package prefetch

// Prefetcher warms the cache for a single tag. Implemented by *node.Node.
type Prefetcher interface {
	Prefetch(tag string) error
}

// Warmer turns model predictions into proactive cache fills. Best-effort: a
// failed prefetch is skipped, never propagated.
type Warmer struct {
	pf    Prefetcher
	model *Model
	k     int
}

// NewWarmer builds a Warmer that warms up to k predictions per seed.
func NewWarmer(pf Prefetcher, model *Model, k int) *Warmer {
	return &Warmer{pf: pf, model: model, k: k}
}

// WarmFor predicts likely-next tags for seed and prefetches each, returning the
// tags successfully warmed (in prediction order).
func (w *Warmer) WarmFor(seed string) []string {
	preds := w.model.Predict(seed, w.k)
	warmed := make([]string, 0, len(preds))
	for _, tag := range preds {
		if err := w.pf.Prefetch(tag); err == nil {
			warmed = append(warmed, tag)
		}
	}
	return warmed
}
```

- [ ] **Step 4: Run → pass** — `go test ./internal/prefetch/ -v`.
- [ ] **Step 5: Commit** — `git commit -m "feat(burrow): best-effort prefetch warmer"`

---

## Task 4: Integration — warmer drives real node to pull from a peer

**Files:** Create `burrow/internal/httpapi/prefetch_test.go`

Prove the whole chain: a model predicts a tag, the warmer calls the real node's `Prefetch`, and the node pulls that artifact's chunks from a peer — so a later serve needs no origin/peer.

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
	"burrow/internal/prefetch"
)

func TestWarmerPrefetchesFromPeer(t *testing.T) {
	data := bytes.Repeat([]byte("WARM"), chunk.Size)

	// Node B: source with the artifact, serving chunks.
	od := t.TempDir()
	os.WriteFile(filepath.Join(od, "sbx-0.33.0.tar.gz"), data, 0o644)
	bdir := t.TempDir()
	bStore, _ := cas.New(filepath.Join(bdir, "blobs"))
	bIdx, _ := index.New(bStore, filepath.Join(bdir, "tags.json"))
	bNode := node.New(bStore, bIdx, origin.DirOrigin{Dir: od})
	bSrv := httptest.NewServer(httpapi.NewServer(bNode))
	defer bSrv.Close()
	bNode.Artifact("sbx@0.33.0") // prime B

	// Node A: manifest only, empty origin, peers -> B.
	adir := t.TempDir()
	aStore, _ := cas.New(filepath.Join(adir, "blobs"))
	aIdx, _ := index.New(aStore, filepath.Join(adir, "tags.json"))
	aNode := node.New(aStore, aIdx, origin.DirOrigin{Dir: t.TempDir()})
	chunks, _ := chunk.Split(bytes.NewReader(data))
	hashes := make([]string, len(chunks))
	for i, c := range chunks {
		hashes[i] = cas.HashOf(c)
	}
	m := &manifest.Manifest{Name: "sbx", Version: "0.33.0", Size: int64(len(data)), Chunks: hashes}
	mh, _ := aIdx.PutManifest(m)
	aIdx.SetTag("sbx@0.33.0", mh)
	aNode.SetPeers(peer.NewFetcher(peer.NewSet(bSrv.URL), bSrv.Client()))

	// Model learns that seed "task:build" co-occurs with the sbx tag.
	model := prefetch.NewModel()
	model.Observe("hist1", "task:build")
	model.Observe("hist1", "sbx@0.33.0")

	w := prefetch.NewWarmer(aNode, model, 5)
	warmed := w.WarmFor("task:build")

	if len(warmed) != 1 || warmed[0] != "sbx@0.33.0" {
		t.Fatalf("warmed=%v want [sbx@0.33.0]", warmed)
	}
	// Warming pulled the chunks into A ahead of any serve.
	for _, h := range hashes {
		if !aStore.Has(h) {
			t.Fatalf("chunk %s not warmed into A", h)
		}
	}
}
```

- [ ] **Step 2: Run → pass** — `go test ./internal/httpapi/ -run TestWarmerPrefetchesFromPeer -v`.
- [ ] **Step 3: Full suite** — `go test ./...`.
- [ ] **Step 4: Commit** — `git commit -m "feat(burrow): warmer drives node prefetch from peer (integration)"`

---

## Done criteria

- `go build ./...`, `go vet ./...`, `go test ./...` all pass.
- `Model.Predict` ranks co-accessed tags deterministically (count desc, name asc), excludes seed/unknown.
- `Warmer` warms predictions and is best-effort (a failure skips, never stops the rest).
- Integration: a model prediction causes the real node to pull an artifact's chunks from a peer before demand.

## Next plan

Plan #6 — self-managing control plane: operator agents watch hit-rate/health/range-load and rebalance, scale, evict, and heal under policy guardrails (propose → check → act).
