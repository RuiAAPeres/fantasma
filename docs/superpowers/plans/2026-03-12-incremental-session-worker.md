# Incremental Session Worker Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace bounded historical session recomputation with a tail-only incremental worker model that keeps `sessions` and `session_daily` on predictable forward-only paths.

**Architecture:** `fantasma-worker` keeps one mutable tail session per `(project_id, install_id)` in persisted `install_session_state`, updates only that tail or inserts a new session, and ignores older-than-tail events for derived-state mutation. `session_daily` is updated from explicit session deltas instead of rebuilds from `sessions`, and the public daily API is narrowed to metrics that can be maintained incrementally without distinct-membership recomputation.

**Tech Stack:** Rust 2024, Tokio, Axum, SQLx/Postgres migrations, Chrono, workspace unit/integration tests.

---

## Chunk 1: Tail-State Store Surface

### Task 1: Replace recompute-oriented store tests with tail-state expectations

**Files:**
- Modify: `crates/fantasma-store/src/lib.rs`
- Modify: `docs/STATUS.md`

- [x] Add `docs/STATUS.md` active-work note that the branch is being redesigned from recompute-based sessionization to incremental tail-state processing.
- [x] Rewrite store tests first to fail on the current code for:
  - migrations create `install_session_state`
  - load/save tail state round-trip
  - insert-new-session + initialize tail state
  - extend-tail-session updates one existing session row in place
  - `session_daily` increments for new session inserts
  - `session_daily` adds only duration delta for tail extension
- [x] Run: `CARGO_TARGET_DIR=/Users/ruiperes/Code/fantasma/target cargo test -p fantasma-store --no-run`
- [x] Run one targeted DB-backed store test command to confirm runtime verification is blocked only by missing `DATABASE_URL` when applicable`.

### Task 2: Add migrations and store APIs for incremental state

**Files:**
- Create: `crates/fantasma-store/migrations/0003_install_session_state.sql`
- Modify: `crates/fantasma-store/migrations/0002_session_daily.sql`
- Modify: `crates/fantasma-store/src/lib.rs`

- [x] Add `install_session_state(project_id, install_id, tail_session_id, tail_session_start, tail_session_end, tail_event_count, tail_duration_seconds, tail_day, updated_at)` with `(project_id, install_id)` primary key.
- [x] Narrow `session_daily` to `project_id, day, sessions_count, total_duration_seconds, updated_at`.
- [x] Add store types and helpers for:
  - load install tail state
  - upsert install tail state
  - insert session row
  - update existing tail session row
  - increment/upsert `session_daily` for new session insert
  - increment-only duration delta for tail extension
- [x] Remove or deprecate store helpers that exist only for recompute-window repair or `session_daily` day rebuilds.
- [x] Run: `CARGO_TARGET_DIR=/Users/ruiperes/Code/fantasma/target cargo test -p fantasma-store --no-run`

## Chunk 2: Incremental Worker Rewrite

### Task 3: Replace recompute-window worker tests with tail-only behavior

**Files:**
- Modify: `crates/fantasma-worker/src/worker.rs`

- [x] Replace recompute-based worker tests with failing tests for:
  - first event creates a session and tail state
  - in-order event within 30 minutes extends the current tail session
  - in-order event beyond 30 minutes starts a new session and replaces tail state
  - older-than-tail event leaves `sessions` unchanged
  - `session_daily` updates incrementally for both insert and extension paths
  - cross-midnight extension remains bucketed on `session_start` day
- [x] Run: `CARGO_TARGET_DIR=/Users/ruiperes/Code/fantasma/target cargo test -p fantasma-worker --no-run`

### Task 4: Rewrite worker processing around persisted tail state

**Files:**
- Modify: `crates/fantasma-worker/src/worker.rs`
- Modify: `crates/fantasma-worker/src/lib.rs`
- Modify: `crates/fantasma-worker/src/main.rs`

- [x] Delete recompute-window loading, overlapping-session scans, historical raw-event reloads, and delete/rebuild session paths.
- [x] Implement forward-only grouped batch processing:
  - load tail state once per `(project_id, install_id)`
  - process sorted events in `(timestamp, id)` order
  - extend tail only when `timestamp >= tail_end` and gap `<= 30 minutes`
  - start new session only when `timestamp > tail_end` and gap `> 30 minutes`
  - ignore older-than-tail events for derived-state mutation
- [x] Add one brief comment in code documenting the older-than-tail rule.
- [x] Keep `events_raw` append-only behavior unchanged.
- [x] Run: `CARGO_TARGET_DIR=/Users/ruiperes/Code/fantasma/target cargo test -p fantasma-worker --no-run`

## Chunk 3: Narrowed Daily API and Verification

### Task 5: Narrow public daily metrics scope

**Files:**
- Modify: `crates/fantasma-core/src/metrics.rs`
- Modify: `crates/fantasma-api/src/http.rs`
- Modify: `crates/fantasma-api/src/main.rs`
- Modify: `schemas/openapi/fantasma.yaml`

- [x] Remove `active_installs_daily` from the daily-series slice.
- [x] Keep summary routes unchanged.
- [x] Keep only:
  - `/v1/metrics/sessions/count/daily`
  - `/v1/metrics/sessions/duration/total/daily`
- [x] Preserve inclusive `start_date` / `end_date` and `422` invalid-range behavior.
- [x] Update API tests first, then implement the narrowed routes.
- [x] Run: `CARGO_TARGET_DIR=/Users/ruiperes/Code/fantasma/target cargo test -p fantasma-api`

### Task 6: Update end-to-end verification and docs

**Files:**
- Modify: `crates/fantasma-worker/tests/pipeline.rs`
- Modify: `scripts/compose-smoke.sh`
- Modify: `AGENTS.md`
- Modify: `docs/architecture.md`
- Modify: `docs/STATUS.md`
- Modify: `docs/deployment.md`

- [x] Update the in-process pipeline test to assert the narrowed daily endpoints under the incremental model.
- [x] Update the smoke script to poll only the surviving daily endpoints.
- [x] Add the explicit engineering ethos text to `AGENTS.md`, `docs/architecture.md`, `docs/STATUS.md`, and `docs/deployment.md`.
- [x] Record in `docs/STATUS.md` that bounded recomputation was removed in favor of tail-only processing.
- [x] Run:
  - `cargo fmt --all`
  - `CARGO_TARGET_DIR=/Users/ruiperes/Code/fantasma/target cargo test -p fantasma-auth`
  - `CARGO_TARGET_DIR=/Users/ruiperes/Code/fantasma/target cargo test -p fantasma-core`
  - `CARGO_TARGET_DIR=/Users/ruiperes/Code/fantasma/target cargo test -p fantasma-api`
  - `CARGO_TARGET_DIR=/Users/ruiperes/Code/fantasma/target cargo test --workspace --no-run`
- [x] Run one DB-backed worker/integration test command and record the actual result. If it still fails for missing `DATABASE_URL` or Docker health, report that limitation explicitly instead of claiming full runtime verification.
