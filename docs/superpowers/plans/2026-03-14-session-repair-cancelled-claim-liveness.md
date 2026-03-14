# Session Repair Cancelled Claim Liveness Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prevent already-claimed repair jobs from getting stranded when a peer in the same repair batch fails.

**Architecture:** Keep the current “return the first repair-batch error” contract, but stop aborting already-claimed peer workers. Instead, let claimed workers finish their normal success or failure cleanup, then drain shared-bucket finalization and return the recorded error. Reproduce the hole with one blocked claimed repair and one independent failing repair in the same batch.

**Tech Stack:** Rust, sqlx, Postgres, Docker-backed crate tests

---

## Chunk 1: Reproduce The Cancelled-Claim Hole

### Task 1: Add the failing worker regression

**Files:**
- Modify: `crates/fantasma-worker/src/worker.rs`

- [ ] **Step 1: Write the failing test**

Add a `#[sqlx::test]` that:
- creates one valid repair job for an install that will block immediately after claim
- creates one invalid repair job that fails independently
- runs `process_session_repair_batch_with_hooks(..., 2, ...)`
- asserts the batch does not finish before the blocked claimed repair resumes
- resumes the blocked job and verifies the valid claim is not stranded while the failing job is still released for retry

- [ ] **Step 2: Run the test to verify it fails**

Run: `./scripts/docker-test.sh -p fantasma-worker repair_batch_waits_for_claimed_jobs_to_finish_before_returning_error -- --nocapture`
Expected: FAIL because the current batch abort path cancels the blocked claimed worker and returns early.

## Chunk 2: Preserve Claimed Job Liveness

### Task 2: Patch repair-batch coordination

**Files:**
- Modify: `crates/fantasma-worker/src/worker.rs`

- [ ] **Step 1: Implement the minimal production fix**

Stop calling `abort_all()` when a repair task returns an error. Record the first error, continue joining the already-claimed workers so they can finish success or cleanup paths, drain `session_rebuild_queue`, and then return the recorded error.

- [ ] **Step 2: Run the targeted regression**

Run: `./scripts/docker-test.sh -p fantasma-worker repair_batch_waits_for_claimed_jobs_to_finish_before_returning_error -- --nocapture`
Expected: PASS

## Chunk 3: Verify And Record

### Task 3: Run affected verification and update project memory

**Files:**
- Modify: `docs/STATUS.md`
- Modify: `docs/architecture.md`

- [ ] **Step 1: Run affected checks**

Run:
- `./scripts/docker-test.sh -p fantasma-worker --lib -- --quiet`
- `./scripts/docker-test.sh -p fantasma-worker --test pipeline pipeline_keeps_grouped_session_app_version_fixed_after_late_event_repair -- --nocapture`
- `cargo fmt --all --check`
- `cargo clippy --workspace --all-targets -- -D warnings`

Expected: PASS

- [ ] **Step 2: Record completion**

Update `docs/STATUS.md` and `docs/architecture.md` with the cancelled-claim liveness fix summary and fresh verification evidence.
