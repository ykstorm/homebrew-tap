# Burrow — Distributed Binary-Distribution Mesh

**Date:** 2026-06-21
**Status:** Design (approved for planning)
**Origin:** Scaling the `homebrew-tap` single-source distribution model to industry scale with a CockroachDB-inspired, multi-node ("many small heads") caching system that lets an agentic architecture actually scale.

---

## 1. Problem

A Homebrew tap distributes prebuilt binaries (e.g. `sbx`) by pointing every consumer at a single origin URL
(`github.com/docker/sbx-releases/releases/...`). At industry scale — thousands of machines, and AI agents
that pull tools on demand at runtime — that single source becomes a latency, throughput, and availability
bottleneck. We want a distributed cache that:

- serves binaries with low latency from near the consumer,
- survives origin / node / region failure,
- caps origin load regardless of how many agents request at once,
- keeps the existing `brew install` UX working unchanged.

## 2. Core idea — split by consistency need

The system separates into two planes because they have different consistency requirements:

- **Data plane — immutable, content-addressed (sha256).** Tarballs are chunked; every chunk is keyed by its
  own hash. Immutable data can be cached anywhere, served from any peer, and verified on receipt. No consensus
  is needed on bytes.
- **Control plane — metadata index.** The mapping `tag → version → manifest hash` (e.g. resolving `sbx@latest`)
  is a tiny keyspace that needs **strong consistency**. This is guarded by Raft (CockroachDB-style ranges if it
  ever grows; realistically a single small range).

This split is the central design decision: heavy bytes flow consensus-free, only the small pointer index pays
for consensus.

## 3. Topology — three tiers (hybrid)

```
AI agent ─► Tier 1: sidecar (per host, hot LRU, P2P gossip within region)
                │ miss
                ▼
            Tier 2: regional node (warm blob cache; runs Raft metadata cluster)
                │ miss
                ▼
            Tier 3: origin (cold; S3-style content-addressed blob store + tap = publish source of truth)
```

- **Read-through:** each tier populates on the way back from a miss.
- **Peer-to-peer:** same-region sidecars gossip membership and exchange chunks directly (BitTorrent-style)
  before escalating to the regional tier.

## 4. Data plane details

- **Chunking:** tarballs are split into content-addressed chunks. Consecutive versions (`sbx@0.32`, `@0.33`)
  share most chunks, so only the delta transfers. Chunks fetch in parallel from multiple sources.
- **Request coalescing (single-flight):** at every tier, concurrent requests for the same missing chunk
  collapse into one upstream fetch. 1000 agents requesting a just-published binary produce **one** origin
  fetch. Thundering-herd load is bounded independent of agent count.
- **Integrity:** every chunk and manifest is verified against its sha256 on receipt. Caching from untrusted
  peers is therefore safe (tamper-evident). Manifests MAY additionally be signed to authenticate publishers.

## 5. Control plane details (metadata)

- Small Raft cluster co-located on regional nodes.
- `resolve(tag)` is linearizable; **follower reads** within a region serve resolution at low latency.
- **Publish flow:** write chunks + manifest to origin → commit `tag → manifest hash` via Raft → gossip the new
  tag. Data is never invalidated (it is immutable); only the pointer advances.

## 6. Agentic layers

1. **Clients.** Agents call a local sidecar API. Bursty, high-fan-out, unpredictable reads are absorbed by
   coalescing + P2P. No intelligence required in the cache for this layer.
2. **Predictive prefetch.** Telemetry of `task-type → artifacts used` feeds a warming service. On task start and
   on publish, likely-needed manifests + hot chunks are pushed to the region's sidecars. Heuristic first
   (co-occurrence / recency), ML later. **Best-effort: never blocks or alters correctness.**
3. **Self-managing control plane.** Operator agents watch hit-rate, node health, and range load, then rebalance
   Raft ranges, scale regional nodes, evict cold blobs, and heal dead nodes. **Policy-bounded autonomy:**
   propose → guardrail check → act. Not free rein.

## 7. Failure behavior

| Failure | Behavior |
|---|---|
| Origin down | Cached data still served everywhere (immutable). Only *new publishes* are blocked. Degrade, not outage. |
| Regional node loss | Raft tolerates minority loss; data refetched from peers/origin. |
| Sidecar isolated from peers | Falls back to direct regional/origin reads. |
| Request storm | Circuit breakers + single-flight protect origin. |

## 8. Guarantees

- **Data:** immutable + content-addressed → trivially consistent, cacheable anywhere.
- **Metadata:** linearizable via Raft for tag resolution.
- **Prefetch:** eventual, best-effort, never load-bearing for correctness.

## 9. Homebrew bridge (keeps existing UX)

The sidecar exposes a Homebrew-compatible download endpoint. A cask `url` points at the sidecar proxy; the
sidecar resolves and assembles from the mesh. `brew install` works unchanged while the backend silently becomes
the distributed mesh. This is the concrete migration path from "this tap" to "industry scale."

## 10. Testing strategy

- **Unit:** chunker, content-addressed store, manifest resolver.
- **Consensus:** partition / Jepsen-style simulation on the Raft metadata cluster.
- **Burst:** N agents + 1 publish → assert ≤1 origin fetch (single-flight correctness).
- **P2P / gossip:** simulated multi-node cluster under membership churn.
- **Chaos:** kill nodes / partition regions → assert cached data remains available and publishes block safely.
- **Prefetch:** hit-rate uplift measured offline against recorded task traces.

## 11. Candidate tech mapping (non-binding)

- Sidecar / regional daemon: Go.
- Consensus: `hashicorp/raft` or `etcd`.
- Blob store: any S3-compatible object store.
- Membership/gossip: `memberlist`.
- P2P chunk transfer: `libp2p` or a custom chunk protocol.
- Sidecar serves a Homebrew-compatible HTTP download endpoint.

## 12. Explicitly out of scope (YAGNI for v1)

- Multi-region Raft replication of metadata beyond a single small range.
- ML-based prefetch (start heuristic).
- Cross-org / public federation of the mesh.
- Mutable artifacts (everything is immutable + content-addressed by design).
