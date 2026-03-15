# Event Lane Append Optimization Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Improve append freshness on `live-append-small-blobs` by reducing `event_metrics` lane catch-up time without changing public behavior or reviving the reverted scheduler change.

**Architecture:** Start from the kept append attribution state, add only minimal event-lane attribution needed to separate rollup construction from write/commit time, then run a bounded event-lane hypothesis ladder. Keep only candidates that materially improve append checkpoint p95 and absolute event-lane catch-up while preserving correctness and final derived readiness.

**Tech Stack:** Rust, Tokio, SQLx/Postgres, Docker Compose benchmark harness

---

## Chunk 1: Event-Lane Attribution

### Task 1: Add minimal event-lane subphase attribution

**Files:**
- Modify: `crates/fantasma-worker/src/worker.rs`
- Modify: `crates/fantasma-bench/src/main.rs`
- Modify: `docs/STATUS.md`

- [ ] Add append-only event-lane timing that can distinguish rollup construction time from bucket-write/commit time for `process_event_metrics_batch_with_config`.
- [ ] Keep the attribution local to the worker + bench sidecar flow; do not add broad new debug surfaces.
- [ ] Extend the append sidecar summary with the new event-lane subphase fields only if the data is available for the representative append scenario.
- [ ] Run focused tests for the new attribution path.

## Chunk 2: Event-Lane Hypothesis Ladder

### Task 2: Hypothesis A, reduce event-lane write amplification

**Files:**
- Modify: `crates/fantasma-worker/src/worker.rs`
- Modify: `crates/fantasma-store/src/lib.rs`
- Modify: `docs/STATUS.md`

- [ ] Write or extend focused tests around the pure append `event_metrics` write path.
- [ ] Implement one narrow write-amplification reduction if attribution points there.
- [ ] Run the representative append benchmark.
- [ ] Keep or revert based on the append keep rule.

### Task 3: Hypothesis B, reduce event-lane batching or commit overhead

**Files:**
- Modify: `crates/fantasma-worker/src/worker.rs`
- Modify: `crates/fantasma-store/src/lib.rs`
- Modify: `docs/STATUS.md`

- [ ] Only proceed if Hypothesis A is rejected or insufficient and the new attribution points at batch/commit overhead.
- [ ] Add focused tests first, then implement one narrow batching or commit-cadence change.
- [ ] Run the representative append benchmark.
- [ ] Keep or revert based on the append keep rule.

### Task 4: Hypothesis C, refine event-lane subphase attribution only if still ambiguous

**Files:**
- Modify: `crates/fantasma-worker/src/worker.rs`
- Modify: `crates/fantasma-bench/src/main.rs`
- Modify: `docs/STATUS.md`

- [ ] Add one more append-only event-lane timing split only if A/B do not isolate the bottleneck enough to choose a final narrow candidate.
- [ ] Use it to choose one last local follow-up or stop the ladder and report the remaining bottleneck.

## Chunk 3: Verification And Handoff

### Task 5: Verify the best append state and record it

**Files:**
- Modify: `docs/STATUS.md`

- [ ] Run focused tests for the changed event-lane path.
- [ ] Run `cargo test -p fantasma-bench --quiet`.
- [ ] Run `docker compose -f infra/docker/compose.bench.yaml config`.
- [ ] Run `cargo run -p fantasma-bench -- slo --output-dir /tmp/fantasma-bench-append-loop --scenario live-append-small-blobs`.
- [ ] Run `cargo fmt --all --check`.
- [ ] Run `cargo clippy --workspace --all-targets -- -D warnings`.
- [ ] Run `git diff --check`.
- [ ] Update `docs/STATUS.md` with the starting baseline, each hypothesis, keep/revert decisions, and the best proven append state.
