# Session Repair Batch Finalization Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ensure `session_rebuild_queue` still drains queued shared-bucket finalization when a repair batch contains both successful and failed jobs.

**Architecture:** Keep repair jobs concurrent and preserve the current “return the first error” batch behavior, but make batch finalization drain queued rebuild buckets before propagating an error if any repair task already succeeded and queued shared-bucket work. Reproduce the bug with a mixed batch where one repair succeeds first, another fails, and the queued project buckets would otherwise remain stale.

**Tech Stack:** Rust, sqlx, Postgres, Docker-backed crate tests

---

## Chunk 1: Reproduce The Mixed Batch Failure

### Task 1: Add the failing worker regression

**Files:**
- Modify: `crates/fantasma-worker/src/worker.rs`

- [ ] **Step 1: Write the failing test**

Add a `#[sqlx::test]` that:
- creates one valid repair job that queues shared-bucket finalization
- creates one invalid repair job that fails after the successful job commits
- runs `process_session_repair_batch_with_config(..., 2)`
- asserts the batch returns an error but still leaves the successful install's shared project buckets rebuilt

- [ ] **Step 2: Run the test to verify it fails**

Run: `./scripts/docker-test.sh -p fantasma-worker mixed_repair_batch_still_drains_shared_bucket_finalization_before_returning_error -- --nocapture`
Expected: FAIL because the batch exits before draining `session_rebuild_queue`.

## Chunk 2: Drain Finalization Before Returning Errors

### Task 2: Patch repair-batch error handling

**Files:**
- Modify: `crates/fantasma-worker/src/worker.rs`

- [ ] **Step 1: Implement the minimal production fix**

Track whether any repair task processed work in `process_session_repair_batch_inner()`. If a task error or join failure occurs after successful repair work already queued rebuilds, drain `session_rebuild_queue` before returning that error so previously committed shared-bucket work does not remain stale behind a later failing job.

- [ ] **Step 2: Run the targeted regression**

Run: `./scripts/docker-test.sh -p fantasma-worker mixed_repair_batch_still_drains_shared_bucket_finalization_before_returning_error -- --nocapture`
Expected: PASS

## Chunk 3: Verify And Record

### Task 3: Run affected verification and update project memory

**Files:**
- Modify: `docs/STATUS.md`

- [ ] **Step 1: Run affected checks**

Run:
- `./scripts/docker-test.sh -p fantasma-worker --lib -- --quiet`
- `./scripts/docker-test.sh -p fantasma-worker --test pipeline pipeline_keeps_grouped_session_app_version_fixed_after_late_event_repair -- --nocapture`
- `cargo fmt --all --check`
- `cargo clippy --workspace --all-targets -- -D warnings`

Expected: PASS

- [ ] **Step 2: Record completion**

Update `docs/STATUS.md` with the mixed-batch finalization fix summary and fresh verification evidence.
