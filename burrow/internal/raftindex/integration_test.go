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

func TestNodeWithRaftTagStore(t *testing.T) {
	od := t.TempDir()
	want := bytes.Repeat([]byte("R"), 2048)
	if err := os.WriteFile(filepath.Join(od, "sbx-0.33.0.tar.gz"), want, 0o644); err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	cstore, _ := cas.New(filepath.Join(dir, "blobs"))
	manifests, _ := index.New(cstore, filepath.Join(dir, "tags.json"))

	tags, cleanup, err := raftindex.NewInmemSingle()
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()

	idx := raftindex.Combine(tags, manifests)
	n := node.New(cstore, idx, origin.DirOrigin{Dir: od})

	got, err := n.Artifact("sbx@0.33.0")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("bytes via raft-backed node != origin")
	}

	// Tag now resolves through raft consensus.
	if _, ok := tags.Resolve("sbx@0.33.0"); !ok {
		t.Fatal("tag not committed to raft index")
	}
}
