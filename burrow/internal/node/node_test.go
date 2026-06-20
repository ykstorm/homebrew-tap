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
