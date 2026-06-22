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
