// Package httpapi serves Burrow artifacts over HTTP. The /artifact route is
// shaped so a Homebrew cask url can GET it directly and verify the sha256.
package httpapi

import (
	"errors"
	"net/http"

	"burrow/internal/node"
	"burrow/internal/origin"
)

// NewServer returns an http.Handler backed by the given node.
func NewServer(n *node.Node) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("GET /chunk/{hash}", func(w http.ResponseWriter, r *http.Request) {
		data, err := n.GetChunk(r.PathValue("hash"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(data)
	})
	mux.HandleFunc("GET /artifact/{name}/{version}", func(w http.ResponseWriter, r *http.Request) {
		tag := r.PathValue("name") + "@" + r.PathValue("version")
		data, err := n.Artifact(tag)
		if err != nil {
			status := http.StatusInternalServerError
			if errors.Is(err, origin.ErrNotFound) || errors.Is(err, node.ErrBadTag) {
				status = http.StatusNotFound
			}
			http.Error(w, err.Error(), status)
			return
		}
		w.Header().Set("Content-Type", "application/gzip")
		_, _ = w.Write(data)
	})
	return mux
}
