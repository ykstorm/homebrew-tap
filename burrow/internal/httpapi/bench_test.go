package httpapi_test

import (
	"bytes"
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"burrow/internal/cas"
	"burrow/internal/httpapi"
	"burrow/internal/index"
	"burrow/internal/node"
	"burrow/internal/origin"
)

// BenchmarkArtifactCachedParallel measures concurrent read-through serving of a
// warmed artifact -- the common case once a sidecar has the bytes. This is the
// closest in-process proxy for "many agents pulling the same tool at once".
func BenchmarkArtifactCachedParallel(b *testing.B) {
	od := b.TempDir()
	data := bytes.Repeat([]byte("Z"), 256*1024)
	os.WriteFile(filepath.Join(od, "sbx-0.33.0.tar.gz"), data, 0o644)

	dir := b.TempDir()
	s, _ := cas.New(filepath.Join(dir, "blobs"))
	idx, _ := index.New(s, filepath.Join(dir, "tags.json"))
	n := node.New(s, idx, origin.DirOrigin{Dir: od})
	srv := httptest.NewServer(httpapi.NewServer(n))
	defer srv.Close()
	if _, err := n.Artifact("sbx@0.33.0"); err != nil { // warm the cache
		b.Fatal(err)
	}
	client := srv.Client()

	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			resp, err := client.Get(srv.URL + "/artifact/sbx/0.33.0")
			if err != nil {
				b.Fatal(err)
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
	})
}
