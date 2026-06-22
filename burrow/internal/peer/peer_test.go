package peer

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"burrow/internal/cas"
)

func TestFetcherReturnsVerifiedChunk(t *testing.T) {
	body := []byte("chunk-bytes")
	hash := cas.HashOf(body)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/chunk/"+hash {
			w.Write(body)
			return
		}
		http.Error(w, "no", http.StatusNotFound)
	}))
	defer srv.Close()

	f := NewFetcher(NewSet(srv.URL), srv.Client())
	got, ok := f.Fetch(hash)
	if !ok {
		t.Fatal("expected hit")
	}
	if string(got) != string(body) {
		t.Fatalf("got %q", got)
	}
}

func TestFetcherRejectsTamperedChunk(t *testing.T) {
	hash := cas.HashOf([]byte("real"))
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("TAMPERED")) // wrong bytes for the requested hash
	}))
	defer srv.Close()

	f := NewFetcher(NewSet(srv.URL), srv.Client())
	if _, ok := f.Fetch(hash); ok {
		t.Fatal("tampered chunk must be rejected")
	}
}

func TestFetcherMissWhenNoPeerHasIt(t *testing.T) {
	f := NewFetcher(NewSet(), nil)
	if _, ok := f.Fetch(cas.HashOf([]byte("x"))); ok {
		t.Fatal("empty peer set must miss")
	}
}

func TestSetAddRemoveList(t *testing.T) {
	s := NewSet("http://a")
	s.Add("http://b")
	s.Remove("http://a")
	got := s.List()
	if len(got) != 1 || got[0] != "http://b" {
		t.Fatalf("list=%v", got)
	}
}
