# Session Repair Review Fixes Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restore correctness for out-of-order late-event repair windows and preserve the public `process_session_batch()` contract for in-process callers after the session apply/repair split.

**Architecture:** Keep the split worker lanes intact for the scheduler, but make the public helper behave like the old single-lane entrypoint by draining repair work after Stage A. In Stage B, derive repair window bounds from the actual pending-event timestamp extrema instead of the raw-id frontier order used to fetch the events.

**Tech Stack:** Rust, sqlx, Postgres, Docker-backed crate tests

---

## Chunk 1: Reproduce The Regressions

### Task 1: Add the out-of-order repair-window regression

**Files:**
- Modify: `crates/fantasma-worker/src/worker.rs`
- Test: `crates/fantasma-worker/src/worker.rs`

- [ ] **Step 1: Write the failing test**

Add a `#[sqlx::test]` that:
- boots the store
- creates an initial session for one install
- inserts two late events for the same install in raw-id order `02:00`, then `00:50`
- runs Stage A enqueueing plus the repair lane
- asserts the repaired sessions reflect the true time order rather than silently skipping the late events

- [ ] **Step 2: Run test to verify it fails**

Run: `./scripts/docker-test.sh -p fantasma-worker repair_window_uses_timestamp_extrema_for_out_of_order_pending_events -- --nocapture`
Expected: FAIL because the current Stage B logic uses `pending_events.first()/last()` rather than the real timestamp bounds.

### Task 2: Reconfirm the public helper regression

**Files:**
- Test: `crates/fantasma-worker/tests/pipeline.rs`

- [ ] **Step 1: Run the existing reproducer**

Run: `./scripts/docker-test.sh -p fantasma-worker --test pipeline pipeline_keeps_grouped_session_app_version_fixed_after_late_event_repair -- --nocapture`
Expected: FAIL because `process_session_batch()` no longer leaves late-event repairs final.

## Chunk 2: Restore The Contracts

### Task 3: Fix worker timestamp bounds and public helper behavior

**Files:**
- Modify: `crates/fantasma-worker/src/worker.rs`

- [ ] **Step 1: Implement minimal production changes**

Change `repair_install_batch()` so `batch_min_ts` and `batch_max_ts` come from iterating all pending events. Change the public `process_session_batch()` helper so it runs the Stage A batch with the default config, then drains repair jobs to preserve the old in-process semantics without changing scheduler behavior.

- [ ] **Step 2: Run targeted tests to verify green**

Run:
- `./scripts/docker-test.sh -p fantasma-worker repair_window_uses_timestamp_extrema_for_out_of_order_pending_events -- --nocapture`
- `./scripts/docker-test.sh -p fantasma-worker --test pipeline pipeline_keeps_grouped_session_app_version_fixed_after_late_event_repair -- --nocapture`

Expected: PASS

## Chunk 3: Verify And Record

### Task 4: Run affected verification and project-memory updates

**Files:**
- Modify: `docs/STATUS.md`

- [ ] **Step 1: Run the affected checks**

Run:
- `cargo fmt --all --check`
- `cargo clippy --workspace --all-targets -- -D warnings`

Expected: PASS

- [ ] **Step 2: Record completion**

Update `docs/STATUS.md` with the fix summary and fresh verification evidence.
