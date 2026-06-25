# Burrow — External Evaluation Packet

This document is a self-contained brief for an independent LLM (or reviewer) to
evaluate the work. It has no dependency on repository access — everything needed
to judge the design and implementation is inline. Evaluation questions are at the
end.

---

## 1. Original request (verbatim intent)

The user asked to analyze the repo `ykstorm/homebrew-tap` and "discover every
fallback mechanism" related to **data latency, scalability, and big-data analysis
failures** (retries, caches, circuit breakers, mirrors, backoff, etc.), then
produce a gap analysis and a contribution roadmap to make "an agentic
architecture actually scalable."

## 2. What the repo actually is (grounding done first)

`ykstorm/homebrew-tap` is a **Homebrew tap**: 28 files, almost all Ruby Cask
definitions for one prebuilt binary (`sbx`, "Docker Sandboxes"), plus one GitHub
Actions PR-validation workflow and a composite action. PR history (~120 PRs) is
overwhelmingly `chore(cask): update sbx to vX` version bumps; 21 closed-unmerged
PRs are superseded bumps, two "test" PRs, a rename, and doc edits.

**Finding:** there is no big-data pipeline, no data-handling code, and no
resilience subsystems in this repo. The only resilience-adjacent constructs are
shell/cask idioms (`|| true` cleanup, `2>/dev/null`, `next unless File.exist?`,
`fail-fast: false`, `sha256` pinning, `conflicts_with`). The original premise does
not match the codebase.

**Decision (please scrutinize this):** rather than fabricate a fallback report
about systems that do not exist, the work pivoted — with the user's explicit
direction across several turns — to **designing and building** the scalable
distribution system the user actually wanted: "Burrow."

## 3. Burrow — what was built

A distributed binary-distribution mesh that scales the single-origin tap model,
implemented in Go, tested in-process. Six incremental plans, each a working,
tested slice.

### 3.1 Central design decision — split by consistency need

| Plane | Contents | Consistency | Mechanism |
|---|---|---|---|
| **Data** | tarball chunks | immutable, content-addressed by sha256 | cache anywhere, peer-to-peer, **no consensus** |
| **Metadata** | `tag → manifest-hash` (e.g. resolving `sbx@latest`) | **strong / linearizable** | Raft (hashicorp/raft) |

Rationale: immutable content-addressed bytes are trivially safe to cache and move
between untrusted peers (verified by hash on receipt), so they need no consensus.
Only the tiny "which version is current" pointer table needs strong consistency,
which is where Raft is applied. This keeps consensus off the hot, heavy path.

### 3.2 Topology — 3-tier read-through

```
AI agent ─► sidecar (per host; hot cache; P2P with same-region peers)
                │ miss
                ▼
            regional node (warm cache; hosts the Raft metadata cluster)
                │ miss
                ▼
            origin (cold; content-addressed blob store + tap = publish source)
```
Each tier fills on the way back from a miss. Same-region sidecars exchange chunks
directly (BitTorrent-style) before escalating.

### 3.3 Three agentic layers

1. **Clients** — agents pull through a local sidecar; bursty high-fan-out reads
   are absorbed by read-through caching and P2P.
2. **Predictive prefetch** — a co-occurrence model predicts likely-next artifacts
   from access history; a warmer pre-pulls their chunks. Best-effort, never
   load-bearing (a failed prefetch is skipped, never propagated).
3. **Self-managing control plane** — an operator observes the cache and acts
   (eviction implemented) under guardrails: propose → guardrail-check → act, with
   a per-tick rate limit and a Report of Proposed/Applied/Skipped (no silent
   truncation). Rebalance/scale/heal are designed as future policies on the same
   `Policy`/`Action` seam.

### 3.4 Language split (please evaluate)

- **Homebrew-facing edge (the Cask) stays Ruby** — `brew` parses Ruby; the only
  change to a cask is its `url`, pointing at the sidecar. `sha256` is unchanged
  because the sidecar serves byte-identical bytes.
- **Mesh daemon is Go**, not Ruby — Ruby's GVL throttles high-fan-out concurrency,
  there is no production-grade Ruby Raft library, and a static Go binary deploys
  as a sidecar with no runtime. The two sides interoperate over **HTTP**, so the
  boundary is a protocol, not a language.

## 4. Package inventory and test evidence

Go module `burrow`, 12 packages. `go build ./...`, `go vet ./...`, and
`go test ./...` are all clean. Notable proofs:

| Package | Responsibility | Key test evidence |
|---|---|---|
| `cas` | content-addressed store; atomic write; integrity-verified read; `List`/`Delete` | corruption detected on read; idempotent Put; temp-file skip |
| `chunk` | fixed-size stream splitter | exact-multiple, remainder, empty, round-trip |
| `manifest` | artifact → ordered chunk hashes; stable content hash | hash stable & content-sensitive |
| `assemble` | Build/Reassemble | round-trip; **automatic chunk dedup**; size-mismatch caught |
| `origin` | source of raw artifacts (`DirOrigin`) | fetch + not-found |
| `index` | single-node tag table (JSON) + manifest store | persists across reopen |
| `raftindex` | Raft-replicated tag index | **real 3-node in-process cluster: leader elected, SetTag on leader replicates to all followers**; FSM snapshot/restore |
| `node` | read-through cache (origin ingest + peer fill + prefetch) | read-through then cached-after-origin-deleted |
| `peer` | peer registry + chunk fetcher | **tampered peer response rejected by hash**; miss when no peer has it |
| `prefetch` | co-occurrence model + warmer | deterministic ranking; best-effort (one failure does not stop the rest) |
| `control` | policy-bounded operator (eviction) | evicts oldest under PinnedGuard/FloorGuard; rate-limit reports Skipped |
| `httpapi` | `/artifact/{name}/{version}`, `/chunk/{hash}`, `/healthz` | **two-node end-to-end: node A holding only the manifest reconstructs the artifact by pulling chunks from peer B**; live sidecar smoke: served sha256 == source sha256 |

Live smoke test (real built binary, not just unit tests):
```
healthz=200
src_sha = 4d5846ec5fa6d5b594bef58842321ee6fe7cb80a03463ab8a0fa003a2dfb36f8
served  = 4d5846ec5fa6d5b594bef58842321ee6fe7cb80a03463ab8a0fa003a2dfb36f8
SHA_MATCH (brew integrity check would PASS)
cached_hit_after_origin_deleted=200
```

## 5. Explicitly deferred (NOT done — be aware)

This is in-process / single-host-tested. The following are designed but not built;
they are packaging/deployment, not new design:

- Real-network Raft transport + on-disk (BoltDB-style) log/snapshot stores. Current
  Raft tests use `InmemTransport` + `InmemStore`.
- Gossip membership auto-discovery (`hashicorp/memberlist`). Peer membership is
  currently an injected static `Set`.
- Bootstrap/join CLI for multi-node clusters.
- HTTP/GitHub `origin` implementation (interface exists; only `DirOrigin` built).
- True LRU eviction (control plane uses oldest-by-mtime, an explicit heuristic).
- Real prefetch ML model (heuristic co-occurrence only) and publish-triggered
  fan-out push.
- No benchmarks yet: scalability/latency claims are architectural, not measured.

## 6. Honesty notes for the evaluator

- The original "fallback discovery report" deliverable was **not** produced as
  asked, because the premise did not fit the repo. This was surfaced, not hidden.
- Burrow is greenfield: it does **not** modify the existing 28 tap files (except an
  example cask under `burrow/examples/`). It lives in a new `burrow/` subdirectory.
- All "scalability" properties are demonstrated by construction and unit/integration
  tests, not by load benchmarks.

## 7. Questions for the external evaluator

Please assess and push back on:

1. **Premise pivot** — was abandoning the literal "analyze fallbacks" task (because
   the repo has none) and building Burrow the right call, or should the analysis
   deliverable have been produced anyway?
2. **Data/metadata consistency split** — is putting only the tag index under Raft
   while moving immutable content-addressed chunks consensus-free sound? Any
   correctness hole (e.g. a tag committed before its chunks are durable anywhere)?
3. **Raft usage** — is the FSM-over-tag-map + leader-only `Apply` + committed-state
   reads a correct use of hashicorp/raft? Is the in-process 3-node test meaningful
   evidence of replication, or does it hide real-network failure modes?
4. **P2P integrity** — is "fetch from any peer, verify sha256 on receipt, discard on
   mismatch" sufficient against malicious peers, or is signing of manifests needed?
5. **Publish atomicity** — the design commits `tag → manifest` via Raft after
   writing chunks/manifest to origin. Is there a window where a resolvable tag
   points at chunks no node can serve? How should that be closed?
6. **Prefetch / control plane** — are "best-effort, never load-bearing" prefetch and
   "propose → guardrail → act" eviction the right safety models? What guardrail is
   missing (e.g., evicting a chunk still referenced by a live manifest)?
7. **Scope & decomposition** — six plans, one subsystem each. Is the decomposition
   sound, and is anything critical missing before this could be called
   production-viable?
8. **What would you test or benchmark first** to falsify the scalability/latency
   claims?

---

### Appendix: where the full artifacts live (if repo access is granted)

- Design spec: `docs/superpowers/specs/2026-06-21-burrow-distributed-cache-design.md`
- Plans #1–#6: `docs/superpowers/plans/2026-06-21-burrow-*.md`
- Code: `burrow/` (Go module), README at `burrow/README.md`
