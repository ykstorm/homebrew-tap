// Package peer provides peer-to-peer chunk exchange between sidecars. Peers are
// untrusted: every fetched chunk is verified against its requested hash.
package peer

import (
	"io"
	"net/http"
	"sort"
	"sync"
	"time"

	"burrow/internal/cas"
)

// Set is a thread-safe registry of peer base URLs.
type Set struct {
	mu    sync.RWMutex
	peers map[string]struct{}
}

// NewSet creates a Set seeded with the given base URLs.
func NewSet(urls ...string) *Set {
	s := &Set{peers: make(map[string]struct{}, len(urls))}
	for _, u := range urls {
		s.peers[u] = struct{}{}
	}
	return s
}

// Add registers a peer base URL.
func (s *Set) Add(url string) {
	s.mu.Lock()
	s.peers[url] = struct{}{}
	s.mu.Unlock()
}

// Remove deregisters a peer base URL.
func (s *Set) Remove(url string) {
	s.mu.Lock()
	delete(s.peers, url)
	s.mu.Unlock()
}

// List returns the peer base URLs in deterministic order.
func (s *Set) List() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.peers))
	for u := range s.peers {
		out = append(out, u)
	}
	sort.Strings(out)
	return out
}

// Fetcher pulls chunks from peers over HTTP.
type Fetcher struct {
	set    *Set
	client *http.Client
}

// NewFetcher builds a Fetcher. A nil client gets a default with a short timeout.
func NewFetcher(set *Set, client *http.Client) *Fetcher {
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	return &Fetcher{set: set, client: client}
}

// Fetch tries each peer's GET /chunk/{hash} in order and returns the first body
// that hashes to hash. A body that does not match is discarded (untrusted peer).
func (f *Fetcher) Fetch(hash string) ([]byte, bool) {
	for _, base := range f.set.List() {
		resp, err := f.client.Get(base + "/chunk/" + hash)
		if err != nil {
			continue
		}
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			continue
		}
		data, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			continue
		}
		if cas.HashOf(data) != hash {
			continue // integrity check failed: reject
		}
		return data, true
	}
	return nil, false
}
