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
