// Package prefetch predicts and pre-warms artifacts an agent task is likely to
// need next. Heuristic now (co-occurrence); ML later behind Model.Predict.
package prefetch

import (
	"sort"
	"sync"
)

// Model is a co-occurrence learner: tags used together in a session reinforce
// each other, and Predict returns the tags most associated with a seed.
type Model struct {
	mu       sync.Mutex
	sessions map[string]map[string]bool // session -> set of tags seen
	pairs    map[string]map[string]int  // tag -> co-tag -> count
}

// NewModel creates an empty Model.
func NewModel() *Model {
	return &Model{
		sessions: map[string]map[string]bool{},
		pairs:    map[string]map[string]int{},
	}
}

// Observe records that tag was used within session. Repeats within the same
// session do not double-count.
func (m *Model) Observe(session, tag string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	set := m.sessions[session]
	if set == nil {
		set = map[string]bool{}
		m.sessions[session] = set
	}
	if set[tag] {
		return
	}
	for other := range set {
		m.inc(tag, other)
		m.inc(other, tag)
	}
	set[tag] = true
}

func (m *Model) inc(a, b string) {
	row := m.pairs[a]
	if row == nil {
		row = map[string]int{}
		m.pairs[a] = row
	}
	row[b]++
}

// Predict returns up to k tags most co-accessed with seed, excluding seed,
// ranked by count desc then name asc (deterministic).
func (m *Model) Predict(seed string, k int) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	row := m.pairs[seed]
	if len(row) == 0 {
		return nil
	}
	type pc struct {
		tag   string
		count int
	}
	list := make([]pc, 0, len(row))
	for tag, c := range row {
		list = append(list, pc{tag, c})
	}
	sort.Slice(list, func(i, j int) bool {
		if list[i].count != list[j].count {
			return list[i].count > list[j].count
		}
		return list[i].tag < list[j].tag
	})
	if k > len(list) {
		k = len(list)
	}
	out := make([]string, k)
	for i := 0; i < k; i++ {
		out[i] = list[i].tag
	}
	return out
}
