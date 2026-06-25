// Package control is Burrow's policy-bounded operator: it observes a node's
// cache and acts (currently eviction) under guardrails that veto unsafe
// proposals. Loop is propose -> guardrail-check -> act.
package control

import (
	"sort"

	"burrow/internal/cas"
)

// Stats is the observed state of a store.
type Stats struct {
	Count int
	Bytes int64
}

// Action is a proposed change. Only KindEvict is implemented; rebalance/scale/
// heal slot in here later.
type Action struct {
	Kind string
	Hash string
	Size int64
}

// KindEvict removes a blob.
const KindEvict = "evict"

// Policy proposes actions from observed state.
type Policy interface {
	Propose(entries []cas.Entry, s Stats) []Action
}

// CapacityPolicy evicts oldest-by-mtime blobs when usage exceeds High, until
// projected usage is at or below Low.
type CapacityPolicy struct {
	High int64
	Low  int64
}

// Propose implements Policy.
func (p CapacityPolicy) Propose(entries []cas.Entry, s Stats) []Action {
	if s.Bytes <= p.High {
		return nil
	}
	sorted := append([]cas.Entry(nil), entries...)
	sort.Slice(sorted, func(i, j int) bool {
		if !sorted[i].ModTime.Equal(sorted[j].ModTime) {
			return sorted[i].ModTime.Before(sorted[j].ModTime)
		}
		return sorted[i].Hash < sorted[j].Hash
	})
	var acts []Action
	proj := s.Bytes
	for _, e := range sorted {
		if proj <= p.Low {
			break
		}
		acts = append(acts, Action{Kind: KindEvict, Hash: e.Hash, Size: e.Size})
		proj -= e.Size
	}
	return acts
}

// Guardrail vetoes unsafe actions. projected is the store state that would
// result if the action were applied.
type Guardrail interface {
	Allow(a Action, projected Stats) bool
}

// PinnedGuard protects specific hashes (e.g. manifests) from eviction.
type PinnedGuard struct {
	Pinned map[string]bool
}

// Allow implements Guardrail.
func (g PinnedGuard) Allow(a Action, _ Stats) bool { return !g.Pinned[a.Hash] }

// FloorGuard refuses to let the store shrink below MinCount blobs.
type FloorGuard struct {
	MinCount int
}

// Allow implements Guardrail.
func (g FloorGuard) Allow(_ Action, projected Stats) bool { return projected.Count >= g.MinCount }

// BlobStore is the subset of *cas.Store the operator manages.
type BlobStore interface {
	List() ([]cas.Entry, error)
	Delete(hash string) error
}

// Report summarizes one Tick. Skipped counts proposals vetoed by a guardrail or
// dropped by the rate limit -- never silently discarded.
type Report struct {
	Proposed int
	Applied  []Action
	Skipped  int
}

// Operator runs propose -> guardrail-check -> act over a BlobStore.
type Operator struct {
	store      BlobStore
	policy     Policy
	guards     []Guardrail
	maxPerTick int // 0 = unlimited
}

// NewOperator builds an Operator. maxPerTick of 0 means no per-tick limit.
func NewOperator(store BlobStore, policy Policy, guards []Guardrail, maxPerTick int) *Operator {
	return &Operator{store: store, policy: policy, guards: guards, maxPerTick: maxPerTick}
}

func statsOf(entries []cas.Entry) Stats {
	s := Stats{Count: len(entries)}
	for _, e := range entries {
		s.Bytes += e.Size
	}
	return s
}

func (o *Operator) allowed(a Action, projected Stats) bool {
	for _, g := range o.guards {
		if !g.Allow(a, projected) {
			return false
		}
	}
	return true
}

// Tick observes the store, proposes actions, applies those that pass the
// guardrails and rate limit, and returns a Report.
func (o *Operator) Tick() (Report, error) {
	entries, err := o.store.List()
	if err != nil {
		return Report{}, err
	}
	proj := statsOf(entries)
	proposed := o.policy.Propose(entries, proj)
	rep := Report{Proposed: len(proposed)}
	for _, a := range proposed {
		if o.maxPerTick > 0 && len(rep.Applied) >= o.maxPerTick {
			rep.Skipped++
			continue
		}
		next := Stats{Count: proj.Count - 1, Bytes: proj.Bytes - a.Size}
		if !o.allowed(a, next) {
			rep.Skipped++
			continue
		}
		if a.Kind == KindEvict {
			if err := o.store.Delete(a.Hash); err != nil {
				rep.Skipped++
				continue
			}
		}
		rep.Applied = append(rep.Applied, a)
		proj = next
	}
	return rep, nil
}
