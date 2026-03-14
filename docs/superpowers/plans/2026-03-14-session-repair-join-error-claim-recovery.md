# Session Repair Join Error Claim Recovery Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Release claimed repair jobs even when a repair worker panics or is externally cancelled after the short claim commit.

**Architecture:** Keep the current repair-batch structure, but attach claim context to each spawned repair worker so the coordinator can identify and release a claimed job from the join-error path. Reproduce the gap with a deterministic post-claim panic hook.

**Tech Stack:** Rust, sqlx, Postgres, Docker-backed crate tests

---

## Chunk 1: Reproduce The Join-Error Gap

### Task 1: Add the failing worker regression

**Files:**
- Modify: `crates/fantasma-worker/src/worker.rs`

- [ ] **Step 1: Write the failing test**

Add a `#[sqlx::test]` that:
- creates a valid repair frontier for one install
- runs `process_session_repair_batch_with_hooks(..., 1, ...)` with a test hook that panics immediately after the short claim commit
- asserts the batch surfaces an error
- verifies the repair frontier remains queued with `claimed_at = NULL` and can be reclaimed

- [ ] **Step 2: Run the test to verify it fails**

Run: `./scripts/docker-test.sh -p fantasma-worker panicked_repair_worker_releases_claim_for_retry -- --nocapture`
Expected: FAIL because the join-error branch has no job context for targeted claim release.

## Chunk 2: Recover Claims From Join Errors

### Task 2: Patch repair-task context tracking

**Files:**
- Modify: `crates/fantasma-worker/src/worker.rs`

- [ ] **Step 1: Implement the minimal production fix**

Track each spawned repair worker's currently claimed job in task-local shared state keyed by Tokio task ID. On normal success or cleanup, clear that state. On any join error, look up the task ID, release the unresolved claim if present, and then return the batch error.

- [ ] **Step 2: Run the targeted regression**

Run: `./scripts/docker-test.sh -p fantasma-worker panicked_repair_worker_releases_claim_for_retry -- --nocapture`
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

Update `docs/STATUS.md` and `docs/architecture.md` with the join-error claim-recovery fix summary and fresh verification evidence.
