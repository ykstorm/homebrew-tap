package assemble

import (
	"bytes"
	"testing"

	"burrow/internal/cas"
)

func benchData(mb int) []byte { return bytes.Repeat([]byte("A"), mb*1024*1024) }

func BenchmarkBuild16MB(b *testing.B) {
	data := benchData(16)
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s, _ := cas.New(b.TempDir()) // fresh store each iter => real chunk+hash+write
		if _, err := Build("sbx", "0.33.0", data, s); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReassemble16MB(b *testing.B) {
	data := benchData(16)
	s, _ := cas.New(b.TempDir())
	m, _ := Build("sbx", "0.33.0", data, s)
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Reassemble(m, s); err != nil {
			b.Fatal(err)
		}
	}
}
