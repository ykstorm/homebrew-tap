// Command sidecar runs a single-node Burrow cache serving artifacts over HTTP.
package main

import (
	"flag"
	"log"
	"net/http"
	"path/filepath"

	"burrow/internal/cas"
	"burrow/internal/httpapi"
	"burrow/internal/index"
	"burrow/internal/node"
	"burrow/internal/origin"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:7777", "listen address")
	cacheDir := flag.String("cache", "burrow-cache", "local cache directory")
	originDir := flag.String("origin", "origin", "directory of {name}-{version}.tar.gz artifacts")
	flag.Parse()

	store, err := cas.New(filepath.Join(*cacheDir, "blobs"))
	if err != nil {
		log.Fatalf("cas: %v", err)
	}
	idx, err := index.New(store, filepath.Join(*cacheDir, "tags.json"))
	if err != nil {
		log.Fatalf("index: %v", err)
	}
	n := node.New(store, idx, origin.DirOrigin{Dir: *originDir})

	log.Printf("burrow sidecar listening on %s (cache=%s origin=%s)", *addr, *cacheDir, *originDir)
	if err := http.ListenAndServe(*addr, httpapi.NewServer(n)); err != nil {
		log.Fatal(err)
	}
}
