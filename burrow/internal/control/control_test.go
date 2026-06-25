package control

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"burrow/internal/cas"
)

func ts(sec int) time.Time { return time.Unix(int64(sec), 0) }

func casRoot(s *cas.Store) string { return s.Root() }

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
