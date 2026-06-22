# Burrow

A distributed binary-distribution mesh: scale a Homebrew-tap-style single-origin
download model to industry scale with a CockroachDB-inspired, multi-node cache so
an agentic architecture can actually scale.

> **Status:** all six planned subsystems implemented in-process and tested
> (`go test ./...` green, 12 packages). The one explicit deferral is production
> deployment wiring — see [Deferred](#deferred).

## Why

A Homebrew tap points every consumer at one origin URL. At scale — thousands of
hosts and AI agents pulling tools on demand — that single source is a latency,
throughput, and availability bottleneck. Burrow puts a distributed cache in front
of it while keeping `brew install` working unchanged.

## Core idea — split by consistency need

| Plane | What | Consistency | Mechanism |
|---|---|---|---|
| **Data** | tarball chunks | immutable, content-addressed (sha256) | cache anywhere, P2P, no consensus |
| **Metadata** | `tag → manifest-hash` (e.g. `sbx@latest`) | strong | Raft |

Heavy bytes flow consensus-free; only the tiny pointer index pays for consensus.

## Topology (3-tier, read-through)

```
AI agent ─► sidecar (per host, hot LRU, P2P gossip)
                │ miss
                ▼
            regional node (warm; runs the Raft metadata cluster)
                │ miss
                ▼
            origin (cold; content-addressed blob store + tap = publish source)
```

## Packages

| Package | Responsibility |
|---|---|
| `internal/cas` | content-addressed blob store; atomic writes, integrity-verified reads, `List`/`Delete` |
| `internal/chunk` | fixed-size stream splitter |
| `internal/manifest` | artifact → ordered chunk hashes + stable content hash |
| `internal/assemble` | `Build`/`Reassemble` bridging the three (automatic chunk dedup) |
| `internal/origin` | where raw artifacts come from (`DirOrigin`; HTTP later, same interface) |
| `internal/index` | single-node `tag → manifest-hash` table (JSON-persisted) + manifest store |
| `internal/raftindex` | Raft-replicated tag index (strong consistency) |
| `internal/node` | read-through cache: resolve → fill (origin/peer) → reassemble |
| `internal/peer` | peer registry + integrity-checked chunk fetcher (P2P) |
| `internal/prefetch` | co-occurrence model + best-effort warmer (predictive prefetch) |
| `internal/control` | policy-bounded operator: propose → guardrail-check → act (eviction) |
| `internal/httpapi` | `GET /artifact/{name}/{version}`, `GET /chunk/{hash}`, `/healthz` |
| `cmd/sidecar` | single-node sidecar entrypoint |

## Three agentic layers

1. **Clients** — agents pull via the local sidecar; bursty high-fan-out reads
   absorbed by read-through + P2P.
2. **Predictive prefetch** — a co-occurrence model predicts likely-next artifacts;
   a warmer pre-pulls their chunks. Best-effort, never load-bearing.
3. **Self-managing control plane** — an operator watches the cache and evicts
   under guardrails (`PinnedGuard`, `FloorGuard`, per-tick rate limit), reporting
   Proposed/Applied/Skipped (no silent truncation). Rebalance/scale/heal slot into
   the same `Policy`/`Action` seam.

## Run the sidecar

```bash
cd burrow
go build -o sidecar ./cmd/sidecar

# Serve artifacts from a local origin dir of {name}-{version}.tar.gz files.
./sidecar -addr 127.0.0.1:7777 -cache ./burrow-cache -origin ./origin

# Fetch (this is exactly what a cask `url` GETs):
curl -fSL http://127.0.0.1:7777/artifact/sbx/0.33.0 -o sbx.tar.gz
```

The served bytes are byte-identical to the upstream tarball, so a cask's `sha256`
is unchanged.

## Homebrew bridge

The sidecar speaks a Homebrew-compatible download endpoint, so the only change to
a cask is its `url`. See [examples/cask/sbx-burrow.rb](examples/cask/sbx-burrow.rb)
— identical to the upstream `sbx` cask except `url` points at the local sidecar.

## Development

```bash
cd burrow
go test ./...     # all packages
go vet ./...
```

## Design & plans

- Spec: [docs/superpowers/specs/2026-06-21-burrow-distributed-cache-design.md](../docs/superpowers/specs/2026-06-21-burrow-distributed-cache-design.md)
- Plans #1–#6: `docs/superpowers/plans/2026-06-21-burrow-*.md`

## Deferred

Production deployment wiring, intentionally out of scope so far (packaging, not
design):

- Real-network Raft transport + on-disk log/snapshot stores (current tests use
  in-memory transport).
- Gossip membership auto-discovery via `hashicorp/memberlist` (peer `Set` is the
  seam; currently injected).
- Bootstrap/join CLI flags for multi-node clusters.
- HTTP/GitHub `origin` implementation (the `origin.Origin` interface is ready).
