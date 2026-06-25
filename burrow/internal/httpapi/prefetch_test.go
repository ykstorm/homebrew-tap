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
