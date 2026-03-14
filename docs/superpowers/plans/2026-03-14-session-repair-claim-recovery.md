# Session Repair Claim Recovery Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ensure a claimed `session_repair_jobs` row is released back to the queue if Stage B fails after the short claim transaction commits.

**Architecture:** Keep the short claim transaction that decouples Stage A from long repair work, but add an explicit failure-unwind path in the worker that opens a new transaction and clears the claim when replay or commit fails. Verify the behavior with a worker regression that forces a post-claim Stage B error and proves the repair row becomes claimable again.

**Tech Stack:** Rust, sqlx, Postgres, Docker-backed crate tests

---

## Chunk 1: Reproduce The Stranded Claim

### Task 1: Add the failing worker regression

**Files:**
- Modify: `crates/fantasma-worker/src/worker.rs`

- [ ] **Step 1: Write the failing test**

Add a `#[sqlx::test]` that:
- bootstraps the store
- inserts a `session_repair_jobs` row for an install with no `install_session_state`
- runs `process_session_repair_batch_with_config()` and asserts it fails
- verifies the repair job is claimable again after the failure instead of remaining stuck with `claimed_at` set

- [ ] **Step 2: Run the test to verify it fails**

Run: `./scripts/docker-test.sh -p fantasma-worker repair_job_failure_releases_claim_for_retry -- --nocapture`
Expected: FAIL because the claimed row stays stranded after the Stage B error.

## Chunk 2: Release Claims On Failure

### Task 2: Patch the repair worker failure path

**Files:**
- Modify: `crates/fantasma-worker/src/worker.rs`

- [ ] **Step 1: Implement the minimal production fix**

Wrap the Stage B replay transaction in `process_next_session_repair_job()` so any error after the claim commit attempts a new cleanup transaction that calls the existing repair-job completion/release helper for the claimed frontier before returning the original error.

- [ ] **Step 2: Run the targeted regression**

Run: `./scripts/docker-test.sh -p fantasma-worker repair_job_failure_releases_claim_for_retry -- --nocapture`
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

Update `docs/STATUS.md` with the stranded-claim fix summary and fresh verification evidence.
