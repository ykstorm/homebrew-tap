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
