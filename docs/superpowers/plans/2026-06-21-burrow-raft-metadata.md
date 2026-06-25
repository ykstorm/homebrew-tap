# Burrow Raft Metadata (plan #3) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the single-node JSON tag table with a Raft-replicated tag index so `latest`-style tag resolution is strongly consistent across multiple nodes — the "metadata index on consensus" half of the design.

**Architecture:** A new `raftindex` package built on `github.com/hashicorp/raft`. A `raft.FSM` holds the `tag → manifest-hash` map; writes go through `raft.Apply` (replicated log), reads come from committed FSM state. The node layer is refactored so its tag backend is an interface (`TagStore`), letting either the single-node `index` or the replicated `raftindex` satisfy it — manifests stay in the immutable CAS (no consensus on bytes, per design). Tests use in-memory transport + stores to run a real multi-node cluster in-process.

**Tech Stack:** Go + `github.com/hashicorp/raft` (InmemTransport, InmemStore, InmemSnapshotStore for tests). Builds on plans #1–#2.

**Out of scope (later):** production network transport + on-disk BoltDB stores + bootstrap/join CLI flags (a deployment task); the regional cache tier wiring; P2P (#4).

---

## File Structure

- Modify: `burrow/internal/node/node.go` — depend on `Index` interface instead of `*index.Index`
- Create: `burrow/internal/raftindex/fsm.go` — `raft.FSM` over the tag map
- Create: `burrow/internal/raftindex/fsm_test.go`
- Create: `burrow/internal/raftindex/store.go` — `Store` wrapping a `raft.Raft`
- Create: `burrow/internal/raftindex/cluster_test.go` — single + multi-node replication
- Modify: `burrow/go.mod` / `go.sum` — record the raft dependency (already added)

---

## Task 1: Node tag/manifest interfaces

**Files:** Modify `burrow/internal/node/node.go`

Refactor so `Node` holds an `Index` interface (tag ops + manifest ops). `*index.Index` already implements it, so existing call sites and tests are unchanged. This is what lets `raftindex` slot in as the tag backend.

- [ ] **Step 1: Replace the concrete index dependency**

Replace the import of `burrow/internal/index` and the `idx *index.Index` field with interfaces declared in `node`:

```go
// TagStore is the strongly-consistent tag → manifest-hash mapping. In a single
// node this is the JSON index; in a cluster it is raftindex.
type TagStore interface {
	Resolve(tag string) (string, bool)
	SetTag(tag, manifestHash string) error
}

// ManifestStore persists and retrieves manifests (content-addressed, no
// consensus needed).
type ManifestStore interface {
	PutManifest(*manifest.Manifest) (string, error)
	Manifest(hash string) (*manifest.Manifest, error)
}

// Index combines both. *index.Index satisfies it; so does a pairing of a
// raftindex.Store (tags) with an index.Index (manifests).
type Index interface {
	TagStore
	ManifestStore
}
```

Change the struct/constructor:

```go
type Node struct {
	store *cas.Store
	idx   Index
	orig  origin.Origin
}

func New(store *cas.Store, idx Index, orig origin.Origin) *Node {
	return &Node{store: store, idx: idx, orig: orig}
}
```

Update imports: drop `burrow/internal/index`, add `burrow/internal/manifest`. The method bodies (`Ingest`, `Artifact`) are unchanged — they already call `idx.Resolve/SetTag/PutManifest/Manifest`.

- [ ] **Step 2: Run existing suite to prove no regression**

Run: `go test ./internal/node/ ./internal/httpapi/ -v`
Expected: PASS (call sites pass `*index.Index`, which satisfies `Index`).

- [ ] **Step 3: Commit** — `git commit -m "refactor(burrow): node depends on Index interface"`

---

## Task 2: Raft FSM over the tag map

**Files:** Create `burrow/internal/raftindex/fsm.go`, `burrow/internal/raftindex/fsm_test.go`

- [ ] **Step 1: Failing test**

```go
package raftindex

import (
	"bytes"
	"io"
	"testing"
)

type nopCloser struct{ io.Reader }

func (nopCloser) Close() error { return nil }

func TestFSMApplySet(t *testing.T) {
	f := newFSM()
	cmd, _ := encodeSet("sbx@latest", "hash1")
	if resp := f.applyBytes(cmd); resp != nil {
		t.Fatalf("apply returned %v", resp)
	}
	if h, ok := f.get("sbx@latest"); !ok || h != "hash1" {
		t.Fatalf("got %q,%v", h, ok)
	}
}

func TestFSMSnapshotRestore(t *testing.T) {
	f := newFSM()
	cmd, _ := encodeSet("sbx@latest", "hash1")
	f.applyBytes(cmd)

	var buf bytes.Buffer
	if err := f.snapshotTo(&buf); err != nil {
		t.Fatal(err)
	}
	f2 := newFSM()
	if err := f2.Restore(nopCloser{bytes.NewReader(buf.Bytes())}); err != nil {
		t.Fatal(err)
	}
	if h, ok := f2.get("sbx@latest"); !ok || h != "hash1" {
		t.Fatalf("restored got %q,%v", h, ok)
	}
}
```

- [ ] **Step 2: Run → fail** — `undefined: newFSM`.

- [ ] **Step 3: Implement**

```go
// Package raftindex is a Raft-replicated tag → manifest-hash index, giving
// strongly-consistent tag resolution across nodes.
package raftindex

import (
	"encoding/json"
	"io"
	"sync"

	"github.com/hashicorp/raft"
)

type command struct {
	Op   string `json:"op"`
	Tag  string `json:"tag"`
	Hash string `json:"hash"`
}

func encodeSet(tag, hash string) ([]byte, error) {
	return json.Marshal(command{Op: "set", Tag: tag, Hash: hash})
}

// fsm is a raft.FSM whose state is the tag table.
type fsm struct {
	mu    sync.RWMutex
	state map[string]string
}

func newFSM() *fsm { return &fsm{state: map[string]string{}} }

// applyBytes is the core mutation, shared by Apply and tests.
func (f *fsm) applyBytes(b []byte) interface{} {
	var c command
	if err := json.Unmarshal(b, &c); err != nil {
		return err
	}
	if c.Op == "set" {
		f.mu.Lock()
		f.state[c.Tag] = c.Hash
		f.mu.Unlock()
	}
	return nil
}

func (f *fsm) get(tag string) (string, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	h, ok := f.state[tag]
	return h, ok
}

// Apply implements raft.FSM.
func (f *fsm) Apply(l *raft.Log) interface{} { return f.applyBytes(l.Data) }

// Snapshot implements raft.FSM.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	cp := make(map[string]string, len(f.state))
	for k, v := range f.state {
		cp[k] = v
	}
	return &snapshot{state: cp}, nil
}

// Restore implements raft.FSM.
func (f *fsm) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	var state map[string]string
	if err := json.NewDecoder(rc).Decode(&state); err != nil {
		return err
	}
	f.mu.Lock()
	f.state = state
	f.mu.Unlock()
	return nil
}

// snapshotTo is a test helper writing the current state as JSON.
func (f *fsm) snapshotTo(w io.Writer) error {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return json.NewEncoder(w).Encode(f.state)
}

type snapshot struct{ state map[string]string }

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	if err := json.NewEncoder(sink).Encode(s.state); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *snapshot) Release() {}
```

- [ ] **Step 4: Run → pass** — `go test ./internal/raftindex/ -run TestFSM -v`.
- [ ] **Step 5: Commit** — `git commit -m "feat(burrow): raft FSM over tag map"`

---

## Task 3: Store wrapper + single-node cluster

**Files:** Create `burrow/internal/raftindex/store.go`, `burrow/internal/raftindex/cluster_test.go`

- [ ] **Step 1: Failing test (single node)**

```go
package raftindex

import (
	"testing"
	"time"
)

func TestSingleNodeSetResolve(t *testing.T) {
	s, cleanup := newTestNode(t, "n1", true)
	defer cleanup()
	waitLeader(t, s)

	if err := s.SetTag("sbx@latest", "deadbeef"); err != nil {
		t.Fatalf("SetTag: %v", err)
	}
	if h, ok := s.Resolve("sbx@latest"); !ok || h != "deadbeef" {
		t.Fatalf("resolve got %q,%v", h, ok)
	}
}
```

- [ ] **Step 2: Run → fail** — `undefined: newTestNode`.

- [ ] **Step 3: Implement Store**

```go
package raftindex

import (
	"fmt"
	"time"

	"github.com/hashicorp/raft"
)

// Store is a strongly-consistent tag index backed by a Raft node. It satisfies
// node.TagStore: SetTag replicates via the Raft log; Resolve reads committed
// FSM state. SetTag only succeeds on the leader (returns raft.ErrNotLeader
// otherwise), which is the correct strong-consistency behavior.
type Store struct {
	raft *raft.Raft
	fsm  *fsm
}

// applyTimeout bounds a replicated write.
const applyTimeout = 5 * time.Second

// SetTag replicates tag → manifestHash through the Raft log.
func (s *Store) SetTag(tag, manifestHash string) error {
	cmd, err := encodeSet(tag, manifestHash)
	if err != nil {
		return err
	}
	f := s.raft.Apply(cmd, applyTimeout)
	if err := f.Error(); err != nil {
		return err
	}
	if resp := f.Response(); resp != nil {
		if e, ok := resp.(error); ok {
			return e
		}
	}
	return nil
}

// Resolve returns the manifest hash for tag from committed state.
func (s *Store) Resolve(tag string) (string, bool) { return s.fsm.get(tag) }

// Leader reports the current leader address (empty if none).
func (s *Store) Leader() string { return string(s.raft.Leader()) }

// Close shuts the Raft node down.
func (s *Store) Close() error { return s.raft.Shutdown().Error() }

func errf(format string, a ...interface{}) error { return fmt.Errorf(format, a...) }
```

- [ ] **Step 4: Implement the test harness**

```go
package raftindex

import (
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

// newTestNode builds an in-memory Raft node. If bootstrap is true it bootstraps
// a single-server cluster (itself). Returns the Store, its transport (for
// wiring multi-node tests), and a cleanup func.
func newTestNode(t *testing.T, id string, bootstrap bool) (*Store, func()) {
	t.Helper()
	s, _, cleanup := newTestNodeT(t, id, bootstrap, nil)
	return s, cleanup
}

func newTestNodeT(t *testing.T, id string, bootstrapSelf bool, servers []raft.Server) (*Store, *raft.InmemTransport, func()) {
	t.Helper()
	cfg := raft.DefaultConfig()
	cfg.LocalID = raft.ServerID(id)
	cfg.HeartbeatTimeout = 50 * time.Millisecond
	cfg.ElectionTimeout = 50 * time.Millisecond
	cfg.LeaderLeaseTimeout = 50 * time.Millisecond
	cfg.CommitTimeout = 5 * time.Millisecond
	cfg.LogLevel = "ERROR"

	f := newFSM()
	logStore := raft.NewInmemStore()
	stable := raft.NewInmemStore()
	snaps := raft.NewInmemSnapshotStore()
	_, trans := raft.NewInmemTransport("")

	r, err := raft.NewRaft(cfg, f, logStore, stable, snaps, trans)
	if err != nil {
		t.Fatalf("NewRaft: %v", err)
	}
	store := &Store{raft: r, fsm: f}

	if bootstrapSelf {
		cfgServers := servers
		if cfgServers == nil {
			cfgServers = []raft.Server{{ID: cfg.LocalID, Address: trans.LocalAddr()}}
		}
		if err := r.BootstrapCluster(raft.Configuration{Servers: cfgServers}).Error(); err != nil {
			t.Fatalf("bootstrap: %v", err)
		}
	}
	cleanup := func() { _ = r.Shutdown().Error() }
	return store, trans, cleanup
}

func waitLeader(t *testing.T, s *Store) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if s.Leader() != "" {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("no leader elected within timeout")
}
```

Note: `newTestNode` calls `newTestNodeT` but discards the transport; the single-node test does not need it. The deadline-poll pattern (not fixed sleeps) keeps the test deterministic under CI jitter.

- [ ] **Step 5: Run → pass** — `go test ./internal/raftindex/ -run TestSingleNode -v`.
- [ ] **Step 6: Commit** — `git commit -m "feat(burrow): raftindex Store + single-node cluster"`

---

## Task 4: Multi-node replication

**Files:** Append to `burrow/internal/raftindex/cluster_test.go`

- [ ] **Step 1: Failing test**

```go
func TestThreeNodeReplication(t *testing.T) {
	ids := []string{"n1", "n2", "n3"}
	stores := make([]*Store, 3)
	transes := make([]*raft.InmemTransport, 3)
	cleanups := make([]func(), 3)

	// Create all three without bootstrapping.
	for i, id := range ids {
		s, tr, cl := newTestNodeT(t, id, false, nil)
		stores[i], transes[i], cleanups[i] = s, tr, cl
	}
	defer func() {
		for _, cl := range cleanups {
			cl()
		}
	}()

	// Fully connect the in-memory transports.
	for i := range transes {
		for j := range transes {
			if i != j {
				transes[i].Connect(transes[j].LocalAddr(), transes[j])
			}
		}
	}

	// Bootstrap the cluster from n1 with all three members.
	servers := make([]raft.Server, 3)
	for i, id := range ids {
		servers[i] = raft.Server{ID: raft.ServerID(id), Address: transes[i].LocalAddr()}
	}
	if err := stores[0].raft.BootstrapCluster(raft.Configuration{Servers: servers}).Error(); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	// Find the leader and write through it.
	leader := waitAnyLeader(t, stores)
	if err := leader.SetTag("sbx@latest", "replicated-hash"); err != nil {
		t.Fatalf("SetTag on leader: %v", err)
	}

	// Every node's committed state must converge.
	for i, s := range stores {
		if !eventuallyResolves(s, "sbx@latest", "replicated-hash") {
			t.Fatalf("node %s did not replicate", ids[i])
		}
	}
}

func waitAnyLeader(t *testing.T, stores []*Store) *Store {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, s := range stores {
			if s.raft.State() == raft.Leader {
				return s
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("no leader elected")
	return nil
}

func eventuallyResolves(s *Store, tag, want string) bool {
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if h, ok := s.Resolve(tag); ok && h == want {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}
```

- [ ] **Step 2: Run → pass** — `go test ./internal/raftindex/ -run TestThreeNodeReplication -v`.
- [ ] **Step 3: Run the whole raftindex package** — `go test ./internal/raftindex/ -v`.
- [ ] **Step 4: Commit** — `git commit -m "feat(burrow): three-node raft replication test"`

---

## Task 5: Integrate raftindex as a node TagStore

**Files:** Create `burrow/internal/raftindex/integration_test.go`

Prove a `Node` works with `raftindex.Store` as its tag backend and `index.Index` as its manifest store, via a small combining adapter.

- [ ] **Step 1: Test**

```go
package raftindex_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"burrow/internal/cas"
	"burrow/internal/index"
	"burrow/internal/node"
	"burrow/internal/origin"
	"burrow/internal/raftindex"
)

// combo pairs a raft tag store with a CAS manifest store to satisfy node.Index.
type combo struct {
	*raftindex.Store     // Resolve, SetTag
	manifests *index.Index // PutManifest, Manifest
}

func (c combo) PutManifest(m *node.ManifestArg) {}

func TestNodeWithRaftTagStore(t *testing.T) {
	// origin
	od := t.TempDir()
	want := bytes.Repeat([]byte("R"), 2048)
	os.WriteFile(filepath.Join(od, "sbx-0.33.0.tar.gz"), want, 0o644)

	// manifest store (CAS) + raft tag store
	dir := t.TempDir()
	cstore, _ := cas.New(filepath.Join(dir, "blobs"))
	manifests, _ := index.New(cstore, filepath.Join(dir, "tags.json"))
	rs, cleanup := raftindex.NewSingleNodeForTest(t)
	defer cleanup()

	idx := raftindex.Combine(rs, manifests)
	n := node.New(cstore, idx, origin.DirOrigin{Dir: od})

	got, err := n.Artifact("sbx@0.33.0")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("bytes via raft-backed node != origin")
	}
}
```

Because the test is in package `raftindex_test`, expose two helpers from the package:

```go
// in store.go (exported test-support, safe for production too):

// Combine pairs a tag Store with a manifest store to satisfy node.Index.
func Combine(tags *Store, manifests interface {
	PutManifest(*manifest.Manifest) (string, error)
	Manifest(string) (*manifest.Manifest, error)
}) node.Index {
	return combined{Store: tags, mstore: manifests}
}

type combined struct {
	*Store
	mstore interface {
		PutManifest(*manifest.Manifest) (string, error)
		Manifest(string) (*manifest.Manifest, error)
	}
}

func (c combined) PutManifest(m *manifest.Manifest) (string, error) { return c.mstore.PutManifest(m) }
func (c combined) Manifest(h string) (*manifest.Manifest, error)    { return c.mstore.Manifest(h) }
```

Add a `NewSingleNodeForTest(t)` helper in `store.go` guarded behind a test-only constructor, or move the bootstrap helper into non-test code. Simplest: add an exported `NewInmemSingle()` that builds a bootstrapped single-node Store for embedding/tests.

> Implementation note: this task wires types together; adjust the exact adapter shape to whatever compiles cleanly against the `node.Index` interface from Task 1. The intent — raft tags + CAS manifests behind one `node.Index` — is the contract to satisfy.

- [ ] **Step 2: Run → pass** — `go test ./internal/raftindex/ -run TestNodeWithRaft -v`.
- [ ] **Step 3: Full suite** — `go test ./...`.
- [ ] **Step 4: Commit** — `git commit -m "feat(burrow): node backed by raft tag store + CAS manifests"`

---

## Done criteria

- `go build ./...`, `go vet ./...`, `go test ./...` all pass.
- A three-node in-process cluster elects a leader, and a `SetTag` on the leader replicates to all followers' committed state.
- A `Node` resolves and serves an artifact using `raftindex.Store` for tags and `index.Index` for manifests.
- `go.mod`/`go.sum` record `github.com/hashicorp/raft` and its deps.

## Next plan

Plan #4 — P2P gossip chunk-swap between sidecars: same-region sidecars discover peers and exchange chunks directly before escalating to the regional tier, cutting origin/regional load.
