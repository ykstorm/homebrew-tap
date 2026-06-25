package cas

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPutGetRoundTrip(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("hello burrow")
	h, err := s.Put(data)
	if err != nil {
		t.Fatal(err)
	}
	if h != HashOf(data) {
		t.Fatalf("Put returned %s, want %s", h, HashOf(data))
	}
	got, err := s.Get(h)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(data) {
		t.Fatalf("Get returned %q, want %q", got, data)
	}
}

func TestPutIsIdempotent(t *testing.T) {
	s, _ := New(t.TempDir())
	h1, _ := s.Put([]byte("x"))
	h2, _ := s.Put([]byte("x"))
	if h1 != h2 {
		t.Fatalf("hashes differ: %s vs %s", h1, h2)
	}
}

func TestGetMissingReturnsNotFound(t *testing.T) {
	s, _ := New(t.TempDir())
	_, err := s.Get("0000000000000000000000000000000000000000000000000000000000000000")
	if err != ErrNotFound {
		t.Fatalf("got %v, want ErrNotFound", err)
	}
}

func TestHasReportsPresence(t *testing.T) {
	s, _ := New(t.TempDir())
	h, _ := s.Put([]byte("y"))
	if !s.Has(h) {
		t.Fatal("Has returned false for stored blob")
	}
	if s.Has("deadbeef") {
		t.Fatal("Has returned true for absent blob")
	}
}

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

func TestGetDetectsCorruption(t *testing.T) {
	dir := t.TempDir()
	s, _ := New(dir)
	h, _ := s.Put([]byte("real"))
	// Tamper with the stored file behind the store's back.
	if err := os.WriteFile(filepath.Join(dir, h), []byte("fake"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(h); err != ErrCorrupt {
		t.Fatalf("got %v, want ErrCorrupt", err)
	}
}
