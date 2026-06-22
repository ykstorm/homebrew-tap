package raftindex

import (
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func TestSingleNodeSetResolve(t *testing.T) {
	s, _, err := newInmem("n1", true, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if !s.waitLeader(3 * time.Second) {
		t.Fatal("no leader elected")
	}
	if err := s.SetTag("sbx@latest", "deadbeef"); err != nil {
		t.Fatalf("SetTag: %v", err)
	}
	if h, ok := s.Resolve("sbx@latest"); !ok || h != "deadbeef" {
		t.Fatalf("resolve got %q,%v", h, ok)
	}
}

func TestThreeNodeReplication(t *testing.T) {
	ids := []string{"n1", "n2", "n3"}
	stores := make([]*Store, 3)
	transes := make([]*raft.InmemTransport, 3)

	for i, id := range ids {
		s, tr, err := newInmem(id, false, nil)
		if err != nil {
			t.Fatalf("newInmem %s: %v", id, err)
		}
		stores[i], transes[i] = s, tr
	}
	defer func() {
		for _, s := range stores {
			_ = s.Close()
		}
	}()

	// Fully connect the in-memory transports.
	for i := range transes {
		for j := range transes {
			if i != j {
				transes[i].Connect(transes[j].LocalAddr(), transes[j])
			}
		}
	}

	// Bootstrap the cluster from n1 with all three members.
	servers := make([]raft.Server, 3)
	for i, id := range ids {
		servers[i] = raft.Server{ID: raft.ServerID(id), Address: transes[i].LocalAddr()}
	}
	if err := stores[0].raft.BootstrapCluster(raft.Configuration{Servers: servers}).Error(); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	leader := waitAnyLeader(t, stores)
	if err := leader.SetTag("sbx@latest", "replicated-hash"); err != nil {
		t.Fatalf("SetTag on leader: %v", err)
	}

	for i, s := range stores {
		if !eventuallyResolves(s, "sbx@latest", "replicated-hash") {
			t.Fatalf("node %s did not replicate", ids[i])
		}
	}
}

func waitAnyLeader(t *testing.T, stores []*Store) *Store {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, s := range stores {
			if s.raft.State() == raft.Leader {
				return s
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("no leader elected")
	return nil
}

func eventuallyResolves(s *Store, tag, want string) bool {
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if h, ok := s.Resolve(tag); ok && h == want {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}
