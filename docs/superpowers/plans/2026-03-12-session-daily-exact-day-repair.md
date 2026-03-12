# Session Daily Exact-Day Repair Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Keep the current fast incremental worker path for in-order events while adding an exact-day transactional repair path for out-of-order install batches so `session_daily` and `session_daily_installs` are rebuilt only for UTC start days that actually changed.

**Architecture:** Current `main` no longer has the old contiguous day-range rebuild flow. The implementation therefore ports the approved exact-day design onto the tail-state worker by adding a bounded recompute path in `crates/fantasma-worker/src/worker.rs`, transaction-scoped overlapping-session reads in `crates/fantasma-store/src/lib.rs`, and exact-day rebuild helpers that preserve `DATE(session_start AT TIME ZONE 'UTC')` attribution. Normal append-only batches stay on the existing incremental path.

**Tech Stack:** Rust, Tokio, sqlx, Postgres, Axum-facing regression tests, Markdown project memory

---

## Chunk 1: Regressions And Current-State Cleanup

### Task 1: Record the current public daily API and fix the rebased pipeline regression

**Files:**
- Modify: `crates/fantasma-worker/tests/pipeline.rs`

- [ ] Write/update the failing integration expectation so the pipeline test no longer expects `GET /v1/metrics/active-installs/daily`.
- [ ] Run the targeted pipeline test and confirm the old expectation fails before the fix.
- [ ] Make the minimal assertion change so the test matches the narrowed public route surface.
- [ ] Re-run the targeted pipeline test and confirm it passes.
- [ ] Commit the cleanup if it becomes large enough to stand alone; otherwise fold it into the exact-day implementation commit.

### Task 2: Add failing worker regressions for exact-day repair behavior

**Files:**
- Modify: `crates/fantasma-worker/src/worker.rs`

- [ ] Write a failing regression that seeds touched days `D1` and `D3`, preserves an unaffected `session_daily` row for `D2`, captures `D2.updated_at`, then proves an out-of-order repair rewrites only `D1` and `D3`.
- [ ] Write a failing cross-midnight regression where a persisted session starts before midnight UTC, overlaps a post-midnight repair window, and is replaced or merged by a late event. Assert the rebuild touches only the session start day.
- [ ] Write a failing delete-window repair regression that proves stale derived rows are removed when old overlapping sessions are replaced.
- [ ] Run only the new worker regressions with the repository Postgres test path and confirm they fail for the expected reasons on current `main`.

## Chunk 2: Store Helpers For Transaction-Scoped Repair

### Task 3: Add transaction-scoped read/write helpers

**Files:**
- Modify: `crates/fantasma-store/src/lib.rs`

- [ ] Add a transaction-scoped helper to fetch overlapping persisted sessions for one `(project_id, install_id)` window.
- [ ] Add a transaction-scoped helper to fetch raw events for one `(project_id, install_id)` window.
- [ ] Add exact-day rebuild helpers that delete and reinsert only the provided UTC start days for `session_daily_installs` and `session_daily`.
- [ ] Keep day attribution aligned with `DATE(session_start AT TIME ZONE 'UTC')`.
- [ ] Add store-level tests for the overlapping-session read helper and exact-day rebuild behavior.
- [ ] Run the targeted store tests and confirm they pass.

## Chunk 3: Worker Exact-Day Repair Path

### Task 4: Add bounded out-of-order repair without regressing the fast path

**Files:**
- Create: `crates/fantasma-worker/src/sessionization.rs`
- Modify: `crates/fantasma-worker/src/lib.rs`
- Modify: `crates/fantasma-worker/src/worker.rs`

- [ ] Introduce a reusable sessionization helper for bounded recompute windows.
- [ ] Detect when an install batch is fully in-order and keep the existing incremental insert/extend path unchanged.
- [ ] Detect when an install batch contains out-of-order events and switch that install to the exact-day repair path.
- [ ] Compute the bounded recompute window from overlapping persisted sessions and the new raw events using the transaction-scoped store helpers.
- [ ] Delete and replace only the affected derived sessions inside the worker transaction.
- [ ] Rebuild only the exact touched UTC start days for the affected project.
- [ ] Preserve install-tail state so later incremental batches continue from the repaired latest session.
- [ ] Re-run the worker regressions and the existing worker tests.

## Chunk 4: Project Memory And Full Verification

### Task 5: Finish project memory and run full verification

**Files:**
- Modify: `docs/STATUS.md`

- [ ] Update `docs/STATUS.md` after the repair path lands, including what changed and what remains open.
- [ ] Run `cargo fmt --all --check`.
- [ ] Run `cargo clippy --workspace --all-targets -- -D warnings`.
- [ ] Run the repository Rust test path with Postgres available.
- [ ] Run `./scripts/compose-smoke.sh`.
- [ ] Commit the exact-day repair work with a clear single-purpose message.
