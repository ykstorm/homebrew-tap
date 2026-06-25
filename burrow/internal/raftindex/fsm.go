// Package raftindex is a Raft-replicated tag → manifest-hash index, giving
// strongly-consistent tag resolution across nodes. Only the small tag table
// goes through consensus; artifact bytes stay in the immutable CAS.
package raftindex

import (
	"encoding/json"
	"io"
	"sync"

	"github.com/hashicorp/raft"
)

type command struct {
	Op   string `json:"op"`
	Tag  string `json:"tag"`
	Hash string `json:"hash"`
}

func encodeSet(tag, hash string) ([]byte, error) {
	return json.Marshal(command{Op: "set", Tag: tag, Hash: hash})
}

// fsm is a raft.FSM whose state is the tag table.
type fsm struct {
	mu    sync.RWMutex
	state map[string]string
}

func newFSM() *fsm { return &fsm{state: map[string]string{}} }

// applyBytes is the core mutation, shared by Apply and tests.
func (f *fsm) applyBytes(b []byte) interface{} {
	var c command
	if err := json.Unmarshal(b, &c); err != nil {
		return err
	}
	if c.Op == "set" {
		f.mu.Lock()
		f.state[c.Tag] = c.Hash
		f.mu.Unlock()
	}
	return nil
}

func (f *fsm) get(tag string) (string, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	h, ok := f.state[tag]
	return h, ok
}

// Apply implements raft.FSM.
func (f *fsm) Apply(l *raft.Log) interface{} { return f.applyBytes(l.Data) }

// Snapshot implements raft.FSM.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	cp := make(map[string]string, len(f.state))
	for k, v := range f.state {
		cp[k] = v
	}
	return &snapshot{state: cp}, nil
}

// Restore implements raft.FSM.
func (f *fsm) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	var state map[string]string
	if err := json.NewDecoder(rc).Decode(&state); err != nil {
		return err
	}
	if state == nil {
		state = map[string]string{}
	}
	f.mu.Lock()
	f.state = state
	f.mu.Unlock()
	return nil
}

// snapshotTo is a test helper writing current state as JSON.
func (f *fsm) snapshotTo(w io.Writer) error {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return json.NewEncoder(w).Encode(f.state)
}

type snapshot struct{ state map[string]string }

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	if err := json.NewEncoder(sink).Encode(s.state); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *snapshot) Release() {}
