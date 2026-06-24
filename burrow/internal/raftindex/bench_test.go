package raftindex

import (
	"testing"
	"time"
)

// BenchmarkResolveCommitted measures concurrent committed-state tag lookups --
// the hot read path agents hit to resolve a tag.
func BenchmarkResolveCommitted(b *testing.B) {
	s, _, err := newInmem("bench", true, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()
	if !s.waitLeader(3 * time.Second) {
		b.Fatal("no leader")
	}
	if err := s.SetTag("sbx@latest", "hash"); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, ok := s.Resolve("sbx@latest"); !ok {
				b.Fatal("resolve missed")
			}
		}
	})
}

// BenchmarkSetTagReplicated measures committed (replicated-log) tag writes.
func BenchmarkSetTagReplicated(b *testing.B) {
	s, _, err := newInmem("bench", true, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()
	if !s.waitLeader(3 * time.Second) {
		b.Fatal("no leader")
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := s.SetTag("sbx@latest", "hash"); err != nil {
			b.Fatal(err)
		}
	}
}
