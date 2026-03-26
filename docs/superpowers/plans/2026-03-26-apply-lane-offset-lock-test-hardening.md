# Apply Lane Offset Lock Test Hardening Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the remaining timing-sensitive raw-offset lock regression with deterministic worker coordination, then rerun the representative append benchmark pair on the exact resulting tree.

**Architecture:** Keep runtime behavior unchanged. Strengthen only the worker regression around claimed repair frontiers so it proves a stronger invariant without polling: the apply lane must finish, persist the widened repair frontier, and release the raw `"sessions"` offset lock while the claimed repair worker is still intentionally blocked. Then rerun the trusted append benchmark pair to confirm the test-only cleanup leaves throughput unchanged.

**Tech Stack:** Rust, Tokio, SQLx, Postgres, `fantasma-worker`, `fantasma-bench`

---

## File Map

- Modify: `crates/fantasma-worker/src/worker.rs`
  Rewrite the flaky offset-lock regression to use deterministic task completion and direct lock acquisition instead of polling sleeps.
- Modify: `docs/STATUS.md`
  Record the start, the verification commands, and the post-hardening benchmark result.
- Create: `docs/superpowers/plans/2026-03-26-apply-lane-offset-lock-test-hardening.md`
  Execution plan for this test-hardening and benchmark follow-up.
- Output: `artifacts/performance/2026-03-26-apply-lane-offset-lock-test-hardening/`
  Representative append benchmark evidence for the exact post-hardening tree.

## Chunk 1: Harden The Regression Without Changing Runtime Logic

### Task 1: Replace the timing-sensitive probe with deterministic assertions

**Files:**
- Modify: `crates/fantasma-worker/src/worker.rs`

- [ ] **Step 1: Strengthen the existing regression scenario**

Keep the blocked repair worker setup, but stop probing `lock_worker_offset(...)` in a sleep loop while the apply worker may or may not still be running.

- [ ] **Step 2: Assert the stronger invariant directly**

Make the test prove:
- the apply batch finishes before the blocked repair resumes
- the blocked repair is still in flight at that point
- a fresh `lock_worker_offset(...)` attempt succeeds after the apply batch finishes but before the repair resumes
- the raw `"sessions"` offset and queued repair frontier reflect the widened follow-up event

- [ ] **Step 3: Run the focused worker regression**

Run:
```bash
DATABASE_URL=postgres://fantasma:fantasma@127.0.0.1:55432/postgres cargo test -p fantasma-worker --lib worker::tests::apply_lane_finishes_and_releases_raw_offset_lock_before_blocked_repair_resumes -- --exact --nocapture
```

Expected:
- the rewritten regression passes without polling sleeps

## Chunk 2: Re-verify The Affected Surface And Benchmark The Exact Tree

### Task 2: Run the worker verification slice

**Files:**
- Modify: `docs/STATUS.md`

- [ ] **Step 1: Run the affected verification**

Run:
```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
DATABASE_URL=postgres://fantasma:fantasma@127.0.0.1:55432/postgres cargo test -p fantasma-worker --lib --quiet
DATABASE_URL=postgres://fantasma:fantasma@127.0.0.1:55432/postgres cargo test -p fantasma-worker --test pipeline --quiet
git diff --check
```

Expected:
- all commands exit `0`

### Task 3: Re-run the representative append benchmark pair

**Files:**
- Modify: `docs/STATUS.md`

- [ ] **Step 1: Run the benchmark pair**

Run:
```bash
FANTASMA_ADMIN_TOKEN=test-token cargo run -p fantasma-bench -- slo --output-dir artifacts/performance/2026-03-26-apply-lane-offset-lock-test-hardening --scenario live-append-small-blobs --scenario live-append-plus-light-repair
```

Expected:
- compare against the current exact-diff prefix-fix checkpoint in `artifacts/performance/2026-03-26-c27-phase1-phase4-stack-rebuild-prefix-fix/summary.md`

- [ ] **Step 2: Record the outcome in `docs/STATUS.md`**

Capture:
- that the change was test-only
- the fresh worker verification commands
- the benchmark delta versus the current exact-diff checkpoint and the accepted Hotspot 3 baseline
