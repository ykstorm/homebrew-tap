package raftindex

import (
	"fmt"
	"time"

	"github.com/hashicorp/raft"

	"burrow/internal/manifest"
	"burrow/internal/node"
)

// Store is a strongly-consistent tag index backed by a Raft node. It satisfies
// node.TagStore: SetTag replicates via the Raft log; Resolve reads committed
// FSM state. SetTag only succeeds on the leader (returns raft.ErrNotLeader
// otherwise) — the correct strong-consistency behavior.
type Store struct {
	raft *raft.Raft
	fsm  *fsm
}

// applyTimeout bounds a replicated write.
const applyTimeout = 5 * time.Second

// SetTag replicates tag → manifestHash through the Raft log.
func (s *Store) SetTag(tag, manifestHash string) error {
	cmd, err := encodeSet(tag, manifestHash)
	if err != nil {
		return err
	}
	f := s.raft.Apply(cmd, applyTimeout)
	if err := f.Error(); err != nil {
		return err
	}
	if resp := f.Response(); resp != nil {
		if e, ok := resp.(error); ok {
			return e
		}
	}
	return nil
}

// Resolve returns the manifest hash for tag from committed state.
func (s *Store) Resolve(tag string) (string, bool) { return s.fsm.get(tag) }

// Leader reports the current leader address (empty if none).
func (s *Store) Leader() string { return string(s.raft.Leader()) }

// Close shuts the Raft node down.
func (s *Store) Close() error { return s.raft.Shutdown().Error() }

// waitLeader blocks until this node is the leader or the timeout elapses.
func (s *Store) waitLeader(d time.Duration) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if s.raft.State() == raft.Leader {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

// newInmem builds an in-memory Raft node (transport + stores). If bootstrapSelf
// is true it bootstraps a cluster with the given servers, or with itself alone
// when servers is nil. It returns the Store, its transport (for wiring
// multi-node clusters), and any error.
func newInmem(id string, bootstrapSelf bool, servers []raft.Server) (*Store, *raft.InmemTransport, error) {
	cfg := raft.DefaultConfig()
	cfg.LocalID = raft.ServerID(id)
	cfg.HeartbeatTimeout = 50 * time.Millisecond
	cfg.ElectionTimeout = 50 * time.Millisecond
	cfg.LeaderLeaseTimeout = 50 * time.Millisecond
	cfg.CommitTimeout = 5 * time.Millisecond
	cfg.LogLevel = "ERROR"

	f := newFSM()
	logStore := raft.NewInmemStore()
	stable := raft.NewInmemStore()
	snaps := raft.NewInmemSnapshotStore()
	_, trans := raft.NewInmemTransport("")

	r, err := raft.NewRaft(cfg, f, logStore, stable, snaps, trans)
	if err != nil {
		return nil, nil, err
	}
	s := &Store{raft: r, fsm: f}
	if bootstrapSelf {
		sv := servers
		if sv == nil {
			sv = []raft.Server{{ID: cfg.LocalID, Address: trans.LocalAddr()}}
		}
		if err := r.BootstrapCluster(raft.Configuration{Servers: sv}).Error(); err != nil {
			_ = r.Shutdown().Error()
			return nil, nil, err
		}
	}
	return s, trans, nil
}

// NewInmemSingle builds a bootstrapped single-node in-memory Store and waits
// for it to become leader. Suitable for embedding in tests and single-host
// deployments. Callers must Close (via the returned func) when done.
func NewInmemSingle() (*Store, func(), error) {
	s, _, err := newInmem("single", true, nil)
	if err != nil {
		return nil, nil, err
	}
	if !s.waitLeader(3 * time.Second) {
		_ = s.Close()
		return nil, nil, fmt.Errorf("raftindex: no leader elected")
	}
	return s, func() { _ = s.Close() }, nil
}

// manifestStore is the subset of the CAS-backed index this package needs.
type manifestStore interface {
	PutManifest(*manifest.Manifest) (string, error)
	Manifest(hash string) (*manifest.Manifest, error)
}

// Combine pairs a raft tag Store with a manifest store to satisfy node.Index:
// strongly-consistent tags + immutable content-addressed manifests.
func Combine(tags *Store, manifests manifestStore) node.Index {
	return combined{Store: tags, mstore: manifests}
}

type combined struct {
	*Store
	mstore manifestStore
}

func (c combined) PutManifest(m *manifest.Manifest) (string, error) { return c.mstore.PutManifest(m) }
func (c combined) Manifest(h string) (*manifest.Manifest, error)    { return c.mstore.Manifest(h) }
