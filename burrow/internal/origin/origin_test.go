package origin

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDirOriginFetch(t *testing.T) {
	dir := t.TempDir()
	want := []byte("fake-tarball-bytes")
	if err := os.WriteFile(filepath.Join(dir, "sbx-0.33.0.tar.gz"), want, 0o644); err != nil {
		t.Fatal(err)
	}
	o := DirOrigin{Dir: dir}
	got, err := o.Fetch("sbx", "0.33.0")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestDirOriginMissing(t *testing.T) {
	o := DirOrigin{Dir: t.TempDir()}
	if _, err := o.Fetch("sbx", "9.9.9"); err != ErrNotFound {
		t.Fatalf("got %v want ErrNotFound", err)
	}
}
