# Session D4 Hot-Path Recovery Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Recover session append throughput by removing the current per-session D4 fanout while preserving first-event session snapshots and insert-once `install_first_seen` semantics.

**Architecture:** Keep the widened snapshot source of truth on `sessions` and `install_first_seen`, but stop maintaining session metric cuboids with per-session incremental deltas. Instead, write sessions normally, collect touched hour/day buckets, and rebuild those session metric buckets set-based in SQL through the shared finalizer. Keep dimension-aware `active_installs`, but serve it from worker-built derived storage rather than aggregating from `sessions` on the request path.

**Tech Stack:** Rust, sqlx, Postgres, Docker-backed Rust tests, `fantasma-bench`

---

## Chunk 1: Recover the write path

### Task 1: Remove incremental D4 fanout from the worker

**Files:**
- Modify: `crates/fantasma-worker/src/worker.rs`
- Modify: `crates/fantasma-store/src/lib.rs`
- Test: `crates/fantasma-worker/tests/pipeline.rs`

- [ ] Replace `SessionMetricAccumulator` usage in the incremental session plan with touched-bucket collection only.
- [ ] Rebuild session metric buckets for touched hour/day buckets through a set-based SQL path after session/first-seen writes succeed.
- [ ] Keep first-event session snapshot semantics and insert-once `install_first_seen` semantics unchanged.
- [ ] Run focused worker/pipeline tests for late repair and `new_installs`.

### Task 2: Keep rebuild/finalization set-based

**Files:**
- Modify: `crates/fantasma-worker/src/worker.rs`
- Modify: `crates/fantasma-store/src/lib.rs`
- Test: `crates/fantasma-store/src/lib.rs`

- [ ] Replace the Rust-side session metric rebuild loop with SQL aggregation over touched `sessions` / `install_first_seen` buckets.
- [ ] Preserve existing session dim3/dim4 bounded read indexes and EXPLAIN-backed tests.
- [ ] Run focused Docker-backed store tests for session metric bucket reads.

## Chunk 2: Preserve grouped `active_installs` off the request path

### Task 3: Keep dimension-aware `active_installs` derived

**Files:**
- Modify: `crates/fantasma-api/src/http.rs`
- Modify: `crates/fantasma-cli/src/app.rs`
- Modify: `crates/fantasma-cli/tests/http_flows.rs`
- Modify: `crates/fantasma-worker/tests/pipeline.rs`
- Modify: `schemas/openapi/fantasma.yaml`
- Modify: `docs/architecture.md`
- Modify: `docs/deployment.md`
- Modify: `docs/performance.md`

- [ ] Keep `active_installs` day-only but preserve D4 filters/grouping.
- [ ] Remove any request-path aggregation from `sessions`; read from worker-built derived slice storage only.
- [ ] Verify CLI/API validation and tests still accept supported filters/grouping for `active_installs`.
- [ ] Update docs to reflect the recovered derived-storage architecture.

## Chunk 3: Verify and benchmark

### Task 4: Re-run the regression suite and performance proof

**Files:**
- Modify: `docs/STATUS.md`
- Output: `artifacts/performance/2026-03-17-session-d4-recovery-*`

- [ ] Run focused Rust and Docker-backed tests for API, store, and worker changes.
- [ ] Run `cargo run -p fantasma-bench -- slo --output-dir artifacts/performance/2026-03-17-session-d4-recovery-representative --scenario live-append-small-blobs`.
- [ ] Run `cargo run -p fantasma-bench -- slo --output-dir artifacts/performance/2026-03-17-session-d4-recovery-mixed --scenario live-append-plus-light-repair`.
- [ ] Run `cargo run -p fantasma-bench -- slo --output-dir artifacts/performance/2026-03-17-session-d4-recovery-reads30 --scenario reads-visibility-30d`.
- [ ] Compare results against the retained March 15 baseline and record the outcome in `docs/STATUS.md`.
