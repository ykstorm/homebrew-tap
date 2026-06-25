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
