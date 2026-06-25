package cas

import (
	"bytes"
	"testing"
)

func BenchmarkPut64K(b *testing.B) {
	s, _ := New(b.TempDir())
	data := bytes.Repeat([]byte("x"), 64*1024)
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Vary the content so every Put is genuine write work, not a dedup skip.
		data[0] = byte(i)
		data[1] = byte(i >> 8)
		data[2] = byte(i >> 16)
		if _, err := s.Put(data); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGet64K(b *testing.B) {
	s, _ := New(b.TempDir())
	data := bytes.Repeat([]byte("y"), 64*1024)
	h, _ := s.Put(data)
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := s.Get(h); err != nil {
			b.Fatal(err)
		}
	}
}
