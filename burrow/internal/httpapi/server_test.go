package httpapi

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"burrow/internal/cas"
	"burrow/internal/index"
	"burrow/internal/node"
	"burrow/internal/origin"
)

func newSrv(t *testing.T, originDir string) http.Handler {
	t.Helper()
	dir := t.TempDir()
	s, _ := cas.New(filepath.Join(dir, "blobs"))
	idx, _ := index.New(s, filepath.Join(dir, "tags.json"))
	n := node.New(s, idx, origin.DirOrigin{Dir: originDir})
	return NewServer(n)
}

func TestServeArtifact(t *testing.T) {
	od := t.TempDir()
	want := []byte("real-tarball")
	os.WriteFile(filepath.Join(od, "sbx-0.33.0.tar.gz"), want, 0o644)
	srv := httptest.NewServer(newSrv(t, od))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/artifact/sbx/0.33.0")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	got, _ := io.ReadAll(resp.Body)
	if string(got) != string(want) {
		t.Fatalf("body %q want %q", got, want)
	}
}

func TestServeArtifactMissing(t *testing.T) {
	srv := httptest.NewServer(newSrv(t, t.TempDir()))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/artifact/sbx/9.9.9")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status %d want 404", resp.StatusCode)
	}
}

func TestHealthz(t *testing.T) {
	srv := httptest.NewServer(newSrv(t, t.TempDir()))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
}
