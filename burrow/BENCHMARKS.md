# Burrow benchmarks

Measured on a laptop (AMD Ryzen 5 4600H, 12 threads, Windows, Go 1.26) with
`go test -bench`. These are in-process numbers — they show how fast the pieces
are, not how a thousand-machine deployment behaves. That larger load test still
needs the real network wiring listed under "Deferred" in the README.

## Results

| What it measures | Result | What it means in plain terms |
|---|---|---|
| Resolve a tag (look up "which version is current") | ~69 ns, 0 allocations | Effectively free — about 14 million lookups a second. The metadata layer is not going to be what slows agents down. |
| Publish a new tag (replicated write through Raft) | ~24 µs | Around 40,000 publishes a second — far more than any tap will ever push. |
| Read a 64 KB chunk from the local cache | ~280 µs (233 MB/s) | Local cache hits are quick. |
| Store a 64 KB chunk | ~4 ms (16 MB/s) | The write path is the slowest piece (disk write + atomic rename). That's expected, and it only happens the first time a chunk is seen. |
| Chunk and hash a 16 MB artifact | ~32 ms (525 MB/s) | A whole binary gets split and hashed in well under a tenth of a second. |
| Rebuild a 16 MB artifact from its chunks | ~40 ms (415 MB/s) | Reassembly plus a full integrity check is fast. |
| Serve a cached artifact over HTTP with many clients at once | ~880 requests/sec of 256 KB each (~226 MB/s) on one node | A single warmed sidecar comfortably handles a busy host's worth of pulls. Real fan-out comes from adding peers and more sidecars. |

## The one that matters

The review's main concern was whether the tag-lookup (Raft) tier becomes the
bottleneck under load. It doesn't. A lookup takes about 69 nanoseconds and
allocates nothing, and writes clear 40,000 a second. The real cost in this system
is moving bytes around — which is exactly why the design keeps bytes off the
consensus path and spreads them between peers.

## Honest limits

- One machine, loopback HTTP, in-memory Raft transport. No network latency, packet
  loss, or disk-backed Raft is exercised here.
- The "many clients at once" figure is concurrent goroutines on the same box, not
  real distributed load across hosts.
- No failure-injection yet — killing the leader under load, dropping packets. That
  is the next milestone.

Reproduce with:

```bash
cd burrow
go test -run '^$' -bench . -benchmem ./...
```
