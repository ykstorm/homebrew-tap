# Burrow Core (CAS + Chunker + Manifest) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the pure, network-free foundation library of Burrow: a content-addressed blob store, a chunker, a manifest format, and assemble/reassemble — the layer every other Burrow subsystem imports.

**Architecture:** A Go module `burrow` with four internal packages. `cas` stores blobs keyed by sha256 and verifies on read. `chunk` splits a byte stream into fixed-size chunks. `manifest` is the JSON descriptor mapping an artifact to its ordered chunk hashes. `assemble` ties them together: `Build` turns bytes into stored chunks + a manifest; `Reassemble` rebuilds the original bytes from a manifest, verifying integrity. Identical chunks across versions collapse to one stored blob (dedup is automatic because keys are content hashes).

**Tech Stack:** Go (stdlib only — `crypto/sha256`, `encoding/json`, `os`, `io`, `testing`). No third-party deps in this layer.

**Scope note:** This plan deliberately excludes networking, Raft, P2P, prefetch, and HTTP serving. Those are later plans (see roadmap in the design spec). This plan must compile and pass `go test ./...` on its own.

---

## File Structure

- Create: `go.mod` — module declaration (`module burrow`, `go 1.22`)
- Create: `internal/cas/store.go` — content-addressed filesystem store
- Create: `internal/cas/store_test.go`
- Create: `internal/chunk/chunker.go` — fixed-size stream splitter
- Create: `internal/chunk/chunker_test.go`
- Create: `internal/manifest/manifest.go` — manifest type + JSON + hash
- Create: `internal/manifest/manifest_test.go`
- Create: `internal/assemble/assemble.go` — Build / Reassemble
- Create: `internal/assemble/assemble_test.go`

Each package has one responsibility and no dependency on the others except `assemble`, which depends on all three.

---

## Task 1: Module init

**Files:**
- Create: `go.mod`

- [ ] **Step 1: Create the module file**

```
module burrow

go 1.22
```

- [ ] **Step 2: Verify it builds (empty module is valid)**

Run: `go build ./...`
Expected: no output, exit 0 (no packages yet, that's fine).

- [ ] **Step 3: Commit**

```bash
git add go.mod
git commit -m "chore(burrow): init go module"
```

---

## Task 2: Content-addressed store (`cas`)

**Files:**
- Create: `internal/cas/store.go`
- Test: `internal/cas/store_test.go`

- [ ] **Step 1: Write the failing test**

```go
package cas

import "testing"

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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/cas/ -run TestPutGetRoundTrip -v`
Expected: FAIL — `undefined: New` / `undefined: HashOf` (package does not compile yet).

- [ ] **Step 3: Write minimal implementation**

```go
// Package cas is a content-addressed blob store keyed by sha256 hex digests.
package cas

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
)

var (
	// ErrNotFound is returned by Get when the blob is absent.
	ErrNotFound = errors.New("cas: blob not found")
	// ErrCorrupt is returned by Get when stored bytes do not match the key.
	ErrCorrupt = errors.New("cas: stored blob hash mismatch")
)

// Store is a content-addressed blob store backed by a directory.
type Store struct {
	root string
}

// New creates a Store rooted at dir, creating it if needed.
func New(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &Store{root: dir}, nil
}

// HashOf returns the sha256 hex digest of data.
func HashOf(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func (s *Store) path(hash string) string { return filepath.Join(s.root, hash) }

// Has reports whether a blob with the given hash is stored.
func (s *Store) Has(hash string) bool {
	_, err := os.Stat(s.path(hash))
	return err == nil
}

// Put stores data and returns its hash. It is idempotent: storing identical
// bytes twice yields the same hash and no duplicate file. The write is atomic
// (temp file + rename) so a crash never leaves a partial blob under its key.
func (s *Store) Put(data []byte) (string, error) {
	hash := HashOf(data)
	p := s.path(hash)
	if _, err := os.Stat(p); err == nil {
		return hash, nil
	}
	tmp, err := os.CreateTemp(s.root, ".tmp-*")
	if err != nil {
		return "", err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return "", err
	}
	if err := tmp.Close(); err != nil {
		return "", err
	}
	if err := os.Rename(tmp.Name(), p); err != nil {
		return "", err
	}
	return hash, nil
}

// Get returns the blob for hash, verifying its integrity. It returns
// ErrNotFound if absent and ErrCorrupt if the stored bytes do not hash to key.
func (s *Store) Get(hash string) ([]byte, error) {
	data, err := os.ReadFile(s.path(hash))
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if HashOf(data) != hash {
		return nil, ErrCorrupt
	}
	return data, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/cas/ -v`
Expected: PASS — all four tests ok.

- [ ] **Step 5: Add the corruption test and verify**

Append to `internal/cas/store_test.go`:

```go
import "os"
import "path/filepath"

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
```

Note: merge the two `import` lines into the existing import block — Go does not allow duplicate `import "testing"` blocks. Final import block:

```go
import (
	"os"
	"path/filepath"
	"testing"
)
```

Run: `go test ./internal/cas/ -v`
Expected: PASS — five tests ok.

- [ ] **Step 6: Commit**

```bash
git add internal/cas/store.go internal/cas/store_test.go
git commit -m "feat(burrow): content-addressed store with integrity verification"
```

---

## Task 3: Chunker (`chunk`)

**Files:**
- Create: `internal/chunk/chunker.go`
- Test: `internal/chunk/chunker_test.go`

- [ ] **Step 1: Write the failing test**

```go
package chunk

import (
	"bytes"
	"testing"
)

func TestSplitExactMultiple(t *testing.T) {
	data := bytes.Repeat([]byte("a"), Size*2)
	chunks, err := Split(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 2 {
		t.Fatalf("got %d chunks, want 2", len(chunks))
	}
	if len(chunks[0]) != Size || len(chunks[1]) != Size {
		t.Fatalf("chunk sizes wrong: %d, %d", len(chunks[0]), len(chunks[1]))
	}
}

func TestSplitWithRemainder(t *testing.T) {
	data := bytes.Repeat([]byte("b"), Size+7)
	chunks, err := Split(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 2 {
		t.Fatalf("got %d chunks, want 2", len(chunks))
	}
	if len(chunks[1]) != 7 {
		t.Fatalf("last chunk size %d, want 7", len(chunks[1]))
	}
}

func TestSplitEmpty(t *testing.T) {
	chunks, err := Split(bytes.NewReader(nil))
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 0 {
		t.Fatalf("got %d chunks, want 0", len(chunks))
	}
}

func TestSplitRoundTrips(t *testing.T) {
	data := bytes.Repeat([]byte("xyz"), Size) // not a chunk multiple
	chunks, _ := Split(bytes.NewReader(data))
	var got []byte
	for _, c := range chunks {
		got = append(got, c...)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("concatenated chunks != original")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/chunk/ -run TestSplitEmpty -v`
Expected: FAIL — `undefined: Split` / `undefined: Size`.

- [ ] **Step 3: Write minimal implementation**

```go
// Package chunk splits a byte stream into fixed-size chunks. Fixed-size is
// intentional for v1 (YAGNI); content-defined chunking can replace it later
// behind the same Split signature.
package chunk

import "io"

// Size is the chunk size in bytes (1 MiB).
const Size = 1 << 20

// Split reads r fully and returns its content as successive chunks of up to
// Size bytes. The final chunk may be smaller. An empty reader yields no chunks.
// Each returned slice owns its bytes (safe to retain).
func Split(r io.Reader) ([][]byte, error) {
	var chunks [][]byte
	buf := make([]byte, Size)
	for {
		n, err := io.ReadFull(r, buf)
		if n > 0 {
			c := make([]byte, n)
			copy(c, buf[:n])
			chunks = append(chunks, c)
		}
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return chunks, nil
		}
		if err != nil {
			return nil, err
		}
	}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/chunk/ -v`
Expected: PASS — four tests ok.

- [ ] **Step 5: Commit**

```bash
git add internal/chunk/chunker.go internal/chunk/chunker_test.go
git commit -m "feat(burrow): fixed-size stream chunker"
```

---

## Task 4: Manifest (`manifest`)

**Files:**
- Create: `internal/manifest/manifest.go`
- Test: `internal/manifest/manifest_test.go`

- [ ] **Step 1: Write the failing test**

```go
package manifest

import "testing"

func TestMarshalUnmarshalRoundTrip(t *testing.T) {
	m := &Manifest{Name: "sbx", Version: "0.33.0", Size: 42, Chunks: []string{"aa", "bb"}}
	data, err := m.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	got, err := Unmarshal(data)
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != m.Name || got.Version != m.Version || got.Size != m.Size {
		t.Fatalf("scalar fields differ: %+v", got)
	}
	if len(got.Chunks) != 2 || got.Chunks[0] != "aa" || got.Chunks[1] != "bb" {
		t.Fatalf("chunks differ: %+v", got.Chunks)
	}
}

func TestHashIsStableAndContentSensitive(t *testing.T) {
	m1 := &Manifest{Name: "sbx", Version: "0.33.0", Size: 1, Chunks: []string{"aa"}}
	m2 := &Manifest{Name: "sbx", Version: "0.33.0", Size: 1, Chunks: []string{"aa"}}
	m3 := &Manifest{Name: "sbx", Version: "0.34.0", Size: 1, Chunks: []string{"aa"}}
	h1, _ := m1.Hash()
	h2, _ := m2.Hash()
	h3, _ := m3.Hash()
	if h1 != h2 {
		t.Fatalf("equal manifests hashed differently: %s vs %s", h1, h2)
	}
	if h1 == h3 {
		t.Fatal("different manifests hashed the same")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/manifest/ -run TestHashIsStableAndContentSensitive -v`
Expected: FAIL — `undefined: Manifest`.

- [ ] **Step 3: Write minimal implementation**

```go
// Package manifest describes an artifact as an ordered list of chunk hashes.
package manifest

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

// Manifest maps a named artifact version to its ordered chunk hashes.
type Manifest struct {
	Name    string   `json:"name"`
	Version string   `json:"version"`
	Size    int64    `json:"size"`
	Chunks  []string `json:"chunks"`
}

// Marshal returns the manifest's JSON encoding. Go's encoding/json emits struct
// fields in declaration order, so this encoding is stable for a given value.
func (m *Manifest) Marshal() ([]byte, error) {
	return json.Marshal(m)
}

// Unmarshal parses a manifest from JSON.
func Unmarshal(data []byte) (*Manifest, error) {
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

// Hash returns the sha256 hex digest of the manifest's JSON encoding. This is
// the manifest-hash the control plane maps a tag to.
func (m *Manifest) Hash() (string, error) {
	data, err := m.Marshal()
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/manifest/ -v`
Expected: PASS — two tests ok.

- [ ] **Step 5: Commit**

```bash
git add internal/manifest/manifest.go internal/manifest/manifest_test.go
git commit -m "feat(burrow): manifest format with content hash"
```

---

## Task 5: Assemble (`assemble`)

**Files:**
- Create: `internal/assemble/assemble.go`
- Test: `internal/assemble/assemble_test.go`

- [ ] **Step 1: Write the failing test**

```go
package assemble

import (
	"bytes"
	"testing"

	"burrow/internal/cas"
	"burrow/internal/chunk"
)

func TestBuildThenReassemble(t *testing.T) {
	s, _ := cas.New(t.TempDir())
	data := bytes.Repeat([]byte("payload"), chunk.Size) // multi-chunk
	m, err := Build("sbx", "0.33.0", data, s)
	if err != nil {
		t.Fatal(err)
	}
	if m.Size != int64(len(data)) {
		t.Fatalf("manifest size %d, want %d", m.Size, len(data))
	}
	got, err := Reassemble(m, s)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("reassembled bytes != original")
	}
}

func TestBuildDedupsIdenticalChunks(t *testing.T) {
	s, _ := cas.New(t.TempDir())
	// Two identical halves -> the two chunks hash equal -> one stored blob.
	half := bytes.Repeat([]byte("z"), chunk.Size)
	data := append(append([]byte{}, half...), half...)
	m, err := Build("sbx", "0.33.0", data, s)
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Chunks) != 2 {
		t.Fatalf("got %d chunk refs, want 2", len(m.Chunks))
	}
	if m.Chunks[0] != m.Chunks[1] {
		t.Fatal("identical chunks should share a hash")
	}
}

func TestReassembleMissingChunkErrors(t *testing.T) {
	s, _ := cas.New(t.TempDir())
	m, _ := Build("sbx", "0.33.0", []byte("small"), s)
	empty, _ := cas.New(t.TempDir()) // store without the chunks
	if _, err := Reassemble(m, empty); err == nil {
		t.Fatal("expected error reassembling from empty store")
	}
}

func TestReassembleDetectsSizeMismatch(t *testing.T) {
	s, _ := cas.New(t.TempDir())
	m, _ := Build("sbx", "0.33.0", []byte("hello"), s)
	m.Size = 9999 // corrupt the declared size
	if _, err := Reassemble(m, s); err != ErrSizeMismatch {
		t.Fatalf("got %v, want ErrSizeMismatch", err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/assemble/ -run TestBuildThenReassemble -v`
Expected: FAIL — `undefined: Build` / `undefined: Reassemble`.

- [ ] **Step 3: Write minimal implementation**

```go
// Package assemble bridges chunk, cas, and manifest: Build turns bytes into
// stored chunks plus a manifest; Reassemble rebuilds bytes from a manifest,
// verifying integrity end to end.
package assemble

import (
	"bytes"
	"errors"

	"burrow/internal/cas"
	"burrow/internal/chunk"
	"burrow/internal/manifest"
)

// ErrSizeMismatch is returned when reassembled bytes do not match the size
// declared in the manifest.
var ErrSizeMismatch = errors.New("assemble: reassembled size mismatch")

// Build chunks data, stores every chunk in store, and returns a manifest
// referencing them in order. Identical chunks dedup automatically in the store.
func Build(name, version string, data []byte, store *cas.Store) (*manifest.Manifest, error) {
	chunks, err := chunk.Split(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	hashes := make([]string, 0, len(chunks))
	for _, c := range chunks {
		h, err := store.Put(c)
		if err != nil {
			return nil, err
		}
		hashes = append(hashes, h)
	}
	return &manifest.Manifest{
		Name:    name,
		Version: version,
		Size:    int64(len(data)),
		Chunks:  hashes,
	}, nil
}

// Reassemble fetches each chunk named by m from store (cas.Get verifies each
// chunk's hash), concatenates them, and checks the total against m.Size.
func Reassemble(m *manifest.Manifest, store *cas.Store) ([]byte, error) {
	var buf bytes.Buffer
	for _, h := range m.Chunks {
		c, err := store.Get(h)
		if err != nil {
			return nil, err
		}
		buf.Write(c)
	}
	out := buf.Bytes()
	if int64(len(out)) != m.Size {
		return nil, ErrSizeMismatch
	}
	return out, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/assemble/ -v`
Expected: PASS — four tests ok.

- [ ] **Step 5: Run the whole suite**

Run: `go test ./...`
Expected: PASS for `burrow/internal/cas`, `.../chunk`, `.../manifest`, `.../assemble`.

- [ ] **Step 6: Commit**

```bash
git add internal/assemble/assemble.go internal/assemble/assemble_test.go
git commit -m "feat(burrow): assemble/reassemble bridging chunk, cas, manifest"
```

---

## Done criteria

- `go build ./...` and `go test ./...` both pass from the module root.
- The four packages have no inter-dependencies except `assemble → {cas, chunk, manifest}`.
- Dedup is demonstrated by `TestBuildDedupsIdenticalChunks`.
- Integrity is enforced at read (`cas.Get`) and at reassembly (size check).

## Next plan

Plan #2 — single-node sidecar: wrap this core with an origin reader (local dir / S3), a local cache, and a Homebrew-compatible HTTP download endpoint serving pinned hashes. That is the first end-to-end runnable Burrow node.
