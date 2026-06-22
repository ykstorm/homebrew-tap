# Burrow Self-Managing Control Plane (plan #6) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give Burrow a policy-bounded operator that watches a node's cache and acts autonomously to keep it healthy — concretely, evicting cold blobs when the store exceeds capacity, under guardrails that veto unsafe actions. This is agentic layer 3: propose → guardrail-check → act, never free rein.

**Architecture:** The CAS gains read/manage primitives (`List`, `Delete`) so a manager can inspect and prune blobs. A `control` package implements the loop as three pure-ish parts: a `Policy` proposes actions from observed stats (`CapacityPolicy` proposes evicting oldest blobs down to a low-watermark when over a high-watermark), `Guardrail`s veto unsafe proposals (`PinnedGuard` protects referenced manifests; `FloorGuard` refuses to shrink below a minimum), and an `Operator.Tick()` runs propose → filter → execute with a per-tick rate limit and a `Report` of what was applied vs skipped (no silent truncation). Eviction is the one action implemented end-to-end; rebalance/scale/heal slot into the same `Policy`/`Action` seam later.

**Tech Stack:** Go stdlib (`os`, `sort`, `time`). Builds on #1 (CAS). Independent of raft/peer/prefetch — operates on any `BlobStore`.

**Design fidelity:** "policy-bounded autonomy (propose → guardrail → act)" from the spec, made literal. Hit-rate/health/range-load policies and multi-node scaling are future `Policy` implementations behind the same interface.

**Out of scope (later):** hit-rate-driven warming policy, raft-range rebalancing, node scale up/down, cross-node heal, true LRU (this uses oldest-by-mtime, an honest heuristic).

---

## File Structure

- Modify: `burrow/internal/cas/store.go` — `Entry`, `List`, `Delete`
- Modify: `burrow/internal/cas/store_test.go` — tests for the new primitives
- Create: `burrow/internal/control/control.go` — Policy, Guardrails, Operator
- Create: `burrow/internal/control/control_test.go`

---

## Task 1: CAS management primitives

**Files:** Modify `burrow/internal/cas/store.go`, `burrow/internal/cas/store_test.go`

- [ ] **Step 1: Failing test** — append to `store_test.go`

```go
func TestListAndDelete(t *testing.T) {
	s, _ := New(t.TempDir())
	ha, _ := s.Put([]byte("aaaa"))
	hb, _ := s.Put([]byte("bb"))

	entries, err := s.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("got %d entries want 2", len(entries))
	}
	sizes := map[string]int64{}
	for _, e := range entries {
		sizes[e.Hash] = e.Size
	}
	if sizes[ha] != 4 || sizes[hb] != 2 {
		t.Fatalf("sizes wrong: %v", sizes)
	}

	if err := s.Delete(ha); err != nil {
		t.Fatal(err)
	}
	if s.Has(ha) {
		t.Fatal("Delete did not remove blob")
	}
	entries, _ = s.List()
	if len(entries) != 1 || entries[0].Hash != hb {
		t.Fatalf("after delete entries=%v", entries)
	}
}

func TestListSkipsTempFiles(t *testing.T) {
	dir := t.TempDir()
	s, _ := New(dir)
	s.Put([]byte("real"))
	// A leftover temp file (as from a crashed Put) must not appear.
	if err := os.WriteFile(filepath.Join(dir, ".tmp-leftover"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	entries, _ := s.List()
	if len(entries) != 1 {
		t.Fatalf("got %d entries want 1 (temp skipped)", len(entries))
	}
}
```

- [ ] **Step 2: Run → fail** — `s.List undefined`.

- [ ] **Step 3: Implement** — add to `store.go` (and `import "time"`, `"strings"`)

```go
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
```

Update the test imports in `store_test.go` if needed (`os`, `path/filepath` are already imported from Task 2 of plan #1).

- [ ] **Step 4: Run → pass** — `go test ./internal/cas/ -v`.
- [ ] **Step 5: Commit** — `git commit -m "feat(burrow): cas List + Delete management primitives"`

---

## Task 2: Policy + guardrails (pure)

**Files:** Create `burrow/internal/control/control.go`, `burrow/internal/control/control_test.go`

- [ ] **Step 1: Failing test**

```go
package control

import (
	"testing"
	"time"

	"burrow/internal/cas"
)

func ts(sec int) time.Time { return time.Unix(int64(sec), 0) }

func TestCapacityPolicyEvictsOldestDownToLow(t *testing.T) {
	entries := []cas.Entry{
		{Hash: "new", Size: 10, ModTime: ts(300)},
		{Hash: "old", Size: 10, ModTime: ts(100)},
		{Hash: "mid", Size: 10, ModTime: ts(200)},
	}
	p := CapacityPolicy{High: 25, Low: 15} // 30 bytes used > 25 => evict to <=15
	acts := p.Propose(entries, Stats{Count: 3, Bytes: 30})
	// Oldest first: evict "old" (->20), still >15, evict "mid" (->10) <=15 stop.
	if len(acts) != 2 || acts[0].Hash != "old" || acts[1].Hash != "mid" {
		t.Fatalf("acts=%v", acts)
	}
}

func TestCapacityPolicyNoopUnderHigh(t *testing.T) {
	p := CapacityPolicy{High: 100, Low: 50}
	if acts := p.Propose(nil, Stats{Count: 0, Bytes: 10}); acts != nil {
		t.Fatalf("expected no actions, got %v", acts)
	}
}

func TestPinnedGuardVetoes(t *testing.T) {
	g := PinnedGuard{Pinned: map[string]bool{"keep": true}}
	if g.Allow(Action{Kind: KindEvict, Hash: "keep"}, Stats{}) {
		t.Fatal("pinned blob must not be evictable")
	}
	if !g.Allow(Action{Kind: KindEvict, Hash: "other"}, Stats{}) {
		t.Fatal("unpinned blob must be allowed")
	}
}

func TestFloorGuardVetoes(t *testing.T) {
	g := FloorGuard{MinCount: 2}
	if g.Allow(Action{Kind: KindEvict}, Stats{Count: 1}) {
		t.Fatal("must veto dropping below MinCount")
	}
	if !g.Allow(Action{Kind: KindEvict}, Stats{Count: 2}) {
		t.Fatal("must allow staying at MinCount")
	}
}
```

- [ ] **Step 2: Run → fail** — `undefined: CapacityPolicy`.

- [ ] **Step 3: Implement**

```go
// Package control is Burrow's policy-bounded operator: it observes a node's
// cache and acts (currently eviction) under guardrails that veto unsafe
// proposals. Loop is propose -> guardrail-check -> act.
package control

import (
	"sort"

	"burrow/internal/cas"
)

// Stats is the observed state of a store.
type Stats struct {
	Count int
	Bytes int64
}

// Action is a proposed change. Only KindEvict is implemented; rebalance/scale/
// heal slot in here later.
type Action struct {
	Kind string
	Hash string
	Size int64
}

// KindEvict removes a blob.
const KindEvict = "evict"

// Policy proposes actions from observed state.
type Policy interface {
	Propose(entries []cas.Entry, s Stats) []Action
}

// CapacityPolicy evicts oldest-by-mtime blobs when usage exceeds High, until
// projected usage is at or below Low.
type CapacityPolicy struct {
	High int64
	Low  int64
}

// Propose implements Policy.
func (p CapacityPolicy) Propose(entries []cas.Entry, s Stats) []Action {
	if s.Bytes <= p.High {
		return nil
	}
	sorted := append([]cas.Entry(nil), entries...)
	sort.Slice(sorted, func(i, j int) bool {
		if !sorted[i].ModTime.Equal(sorted[j].ModTime) {
			return sorted[i].ModTime.Before(sorted[j].ModTime)
		}
		return sorted[i].Hash < sorted[j].Hash
	})
	var acts []Action
	proj := s.Bytes
	for _, e := range sorted {
		if proj <= p.Low {
			break
		}
		acts = append(acts, Action{Kind: KindEvict, Hash: e.Hash, Size: e.Size})
		proj -= e.Size
	}
	return acts
}

// Guardrail vetoes unsafe actions. projected is the store state that would
// result if the action were applied.
type Guardrail interface {
	Allow(a Action, projected Stats) bool
}

// PinnedGuard protects specific hashes (e.g. manifests) from eviction.
type PinnedGuard struct {
	Pinned map[string]bool
}

// Allow implements Guardrail.
func (g PinnedGuard) Allow(a Action, _ Stats) bool { return !g.Pinned[a.Hash] }

// FloorGuard refuses to let the store shrink below MinCount blobs.
type FloorGuard struct {
	MinCount int
}

// Allow implements Guardrail.
func (g FloorGuard) Allow(_ Action, projected Stats) bool { return projected.Count >= g.MinCount }
```

- [ ] **Step 4: Run → pass** — `go test ./internal/control/ -v`.
- [ ] **Step 5: Commit** — `git commit -m "feat(burrow): control plane policy + guardrails"`

---

## Task 3: Operator loop

**Files:** Modify `burrow/internal/control/control.go`, `burrow/internal/control/control_test.go`

- [ ] **Step 1: Failing test** — append to `control_test.go`

```go
import (
	"os"
	"path/filepath"
)

func TestOperatorTickEvictsUnderGuardrails(t *testing.T) {
	s, _ := cas.New(t.TempDir())
	put := func(b []byte, sec int) string {
		h, _ := s.Put(b)
		// Control mtime so "oldest" is deterministic.
		_ = os.Chtimes(filepath.Join(casRoot(s), h), ts(sec), ts(sec))
		return h
	}
	oldH := put([]byte("0000000000"), 100) // 10 bytes, oldest
	midH := put([]byte("1111111111"), 200) // 10 bytes
	pinH := put([]byte("2222222222"), 150) // 10 bytes, pinned (old-ish)
	_ = midH

	op := NewOperator(s, CapacityPolicy{High: 25, Low: 5}, []Guardrail{
		PinnedGuard{Pinned: map[string]bool{pinH: true}},
		FloorGuard{MinCount: 1},
	}, 0)

	rep, err := op.Tick()
	if err != nil {
		t.Fatal(err)
	}
	// 30 bytes > 25. Oldest-first proposes old, pin, mid. Pin is vetoed; floor
	// allows down to 1. So old and mid evicted, pin kept.
	if s.Has(oldH) {
		t.Fatal("oldest should be evicted")
	}
	if !s.Has(pinH) {
		t.Fatal("pinned must survive")
	}
	if len(rep.Applied) == 0 {
		t.Fatal("expected some evictions applied")
	}
	if rep.Skipped == 0 {
		t.Fatal("expected the pinned proposal to be skipped")
	}
}

func TestOperatorRateLimit(t *testing.T) {
	s, _ := cas.New(t.TempDir())
	for i := 0; i < 5; i++ {
		s.Put([]byte{byte('a' + i)}) // 5 one-byte blobs
	}
	op := NewOperator(s, CapacityPolicy{High: 1, Low: 0}, nil, 2) // max 2 per tick
	rep, err := op.Tick()
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Applied) != 2 {
		t.Fatalf("applied=%d want 2 (rate limited)", len(rep.Applied))
	}
	if rep.Skipped == 0 {
		t.Fatal("rate-limited proposals must be reported skipped, not silently dropped")
	}
}
```

Add a tiny test helper exposing the store root (the operator itself never needs it; the test does, to set mtimes):

```go
// in control_test.go
func casRoot(s *cas.Store) string { return s.Root() }
```

This requires a `Root()` accessor on `cas.Store` — add it in store.go:

```go
// Root returns the store's backing directory. Useful for tooling and tests.
func (s *Store) Root() string { return s.root }
```

(Add `Root()` as part of this task; commit it with the operator.)

- [ ] **Step 2: Run → fail** — `undefined: NewOperator`.

- [ ] **Step 3: Implement** — append to `control.go`

```go
// BlobStore is the subset of *cas.Store the operator manages.
type BlobStore interface {
	List() ([]cas.Entry, error)
	Delete(hash string) error
}

// Report summarizes one Tick. Skipped counts proposals vetoed by a guardrail or
// dropped by the rate limit -- never silently discarded.
type Report struct {
	Proposed int
	Applied  []Action
	Skipped  int
}

// Operator runs propose -> guardrail-check -> act over a BlobStore.
type Operator struct {
	store      BlobStore
	policy     Policy
	guards     []Guardrail
	maxPerTick int // 0 = unlimited
}

// NewOperator builds an Operator. maxPerTick of 0 means no per-tick limit.
func NewOperator(store BlobStore, policy Policy, guards []Guardrail, maxPerTick int) *Operator {
	return &Operator{store: store, policy: policy, guards: guards, maxPerTick: maxPerTick}
}

func statsOf(entries []cas.Entry) Stats {
	s := Stats{Count: len(entries)}
	for _, e := range entries {
		s.Bytes += e.Size
	}
	return s
}

func (o *Operator) allowed(a Action, projected Stats) bool {
	for _, g := range o.guards {
		if !g.Allow(a, projected) {
			return false
		}
	}
	return true
}

// Tick observes the store, proposes actions, applies those that pass the
// guardrails and rate limit, and returns a Report.
func (o *Operator) Tick() (Report, error) {
	entries, err := o.store.List()
	if err != nil {
		return Report{}, err
	}
	proj := statsOf(entries)
	proposed := o.policy.Propose(entries, proj)
	rep := Report{Proposed: len(proposed)}
	for _, a := range proposed {
		if o.maxPerTick > 0 && len(rep.Applied) >= o.maxPerTick {
			rep.Skipped++
			continue
		}
		next := Stats{Count: proj.Count - 1, Bytes: proj.Bytes - a.Size}
		if !o.allowed(a, next) {
			rep.Skipped++
			continue
		}
		if a.Kind == KindEvict {
			if err := o.store.Delete(a.Hash); err != nil {
				rep.Skipped++
				continue
			}
		}
		rep.Applied = append(rep.Applied, a)
		proj = next
	}
	return rep, nil
}
```

- [ ] **Step 4: Run → pass** — `go test ./internal/control/ -v`.
- [ ] **Step 5: Full suite + vet** — `go vet ./... && go test ./...`.
- [ ] **Step 6: Commit** — `git commit -m "feat(burrow): policy-bounded operator (propose -> check -> act)"`

---

## Done criteria

- `go build ./...`, `go vet ./...`, `go test ./...` all pass.
- `CapacityPolicy` evicts oldest-first down to the low-watermark only when over the high-watermark.
- `PinnedGuard` protects manifests; `FloorGuard` enforces a minimum; both veto within the operator.
- `Operator.Tick` reports Proposed/Applied/Skipped — rate-limited and vetoed proposals are surfaced, never silently dropped.

## Burrow complete

With #6, all six planned subsystems and all three agentic layers are implemented in-process and tested. The single remaining explicit deferral is production deployment wiring (real network raft transport + on-disk stores + gossip membership via memberlist + bootstrap/join CLI) — a packaging task, not new design.
