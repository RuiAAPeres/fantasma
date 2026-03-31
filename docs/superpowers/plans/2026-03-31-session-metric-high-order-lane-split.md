# Session Metric High-Order Lane Split Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Keep session metric totals plus dim1 on the contract-critical deferred path while moving session dim2/dim3/dim4 cuboids to their own lower-priority deferred queue and drain.

**Architecture:** The existing `session_projection_deferred_rebuild_queue` remains the readiness-critical queue for `session_daily` and primary session metric buckets. A new `session_metric_high_order_deferred_rebuild_queue` coalesces the same touched `(project_id, granularity, bucket_start)` scope for dim2/dim3/dim4 only, and a separate low-priority drain rebuilds those tables after the primary drain has already made totals and dim1 available.

**Tech Stack:** Rust, Tokio, SQLx, Postgres migrations, Docker-backed worker tests, benchmark harness

---

## File Map

- Modify: `crates/fantasma-store/migrations/0023_deferred_projection_rebuild_queues.sql`
  - Add the new high-order session metric deferred queue table.
- Modify: `crates/fantasma-store/src/lib.rs`
  - Add enqueue/claim/delete helpers and tests for the new queue.
- Modify: `crates/fantasma-worker/src/worker.rs`
  - Split the deferred session metric flow into primary and high-order drains, add tests, and update deletion cleanup paths.
- Modify: `crates/fantasma-worker/src/scheduler.rs`
  - Remove the temporary high-order toggle from the main config or repurpose config to match the permanent lane shape.
- Modify: `crates/fantasma-worker/src/main.rs`
  - Remove the temporary env wiring for disabling high-order session metric rebuilds if it is no longer part of the permanent product path.
- Modify: `crates/fantasma-bench/src/main.rs`
  - Remove the temporary benchmark toggle and keep the benchmark aligned with the new default architecture.
- Modify: `docs/architecture.md`
  - Clarify that session totals and 1D catch up sooner, while higher-order session groupings may lag longer.
- Modify: `schemas/openapi/fantasma.yaml`
  - Align public route descriptions with the new freshness model.
- Modify: `docs/STATUS.md`
  - Record the start, result, remaining gap, and benchmark outcome for this permanent split.

## Chunk 1: Lock Failing Tests

### Task 1: Add a failing worker regression for the primary session drain

**Files:**
- Modify: `crates/fantasma-worker/src/worker.rs`

- [ ] **Step 1: Write the failing test**

Add a test that seeds session source-of-truth state plus a `session_projection_deferred_rebuild_queue` entry, runs the primary session projection drain, and asserts:
- `session_metric_buckets_total` and `session_metric_buckets_dim1` are populated
- `session_metric_buckets_dim2`/`dim3`/`dim4` remain empty
- the touched bucket scope is now present in `session_metric_high_order_deferred_rebuild_queue`

- [ ] **Step 2: Run the test to verify it fails**

Run: `./scripts/docker-test.sh -p fantasma-worker --lib primary_session_metric_drain_enqueues_high_order_followup --quiet`

Expected: FAIL because the current code either rebuilds dim2+ inline or has no separate high-order queue to inspect.

### Task 2: Add a failing worker regression for the high-order follow-up drain

**Files:**
- Modify: `crates/fantasma-worker/src/worker.rs`

- [ ] **Step 1: Write the failing test**

Add a test that seeds the new `session_metric_high_order_deferred_rebuild_queue`, runs the new low-priority high-order drain, and asserts dim2/dim3/dim4 rows appear afterward for the touched bucket.

- [ ] **Step 2: Run the test to verify it fails**

Run: `./scripts/docker-test.sh -p fantasma-worker --lib high_order_session_metric_drain_rebuilds_dim2_through_dim4 --quiet`

Expected: FAIL because the dedicated queue/drain does not exist yet.

## Chunk 2: Store And Worker Split

### Task 3: Add the new queue and helper coverage

**Files:**
- Modify: `crates/fantasma-store/migrations/0023_deferred_projection_rebuild_queues.sql`
- Modify: `crates/fantasma-store/src/lib.rs`

- [ ] **Step 1: Add the migration and record types**

Create `session_metric_high_order_deferred_rebuild_queue(project_id, granularity, bucket_start)` with the same coalescing key shape as the primary session projection queue.

- [ ] **Step 2: Add minimal helpers**

Implement:
- enqueue helper
- claim-in-tx helper
- delete-by-project helper
- any queue inspection helpers needed by tests

- [ ] **Step 3: Run focused store helper tests**

Run: `./scripts/docker-test.sh -p fantasma-store session_metric_high_order_deferred_rebuild_queue_round_trips -- --quiet`

Expected: PASS

### Task 4: Split the worker drains

**Files:**
- Modify: `crates/fantasma-worker/src/worker.rs`
- Modify: `crates/fantasma-worker/src/scheduler.rs`
- Modify: `crates/fantasma-worker/src/main.rs`

- [ ] **Step 1: Implement the primary drain behavior**

Change the main session projection deferred drain so it:
- rebuilds `session_daily` as today
- rebuilds only `session_metric_buckets_total` and `session_metric_buckets_dim1`
- enqueues the touched scope into `session_metric_high_order_deferred_rebuild_queue` in the same transaction whenever session metrics are rebuilt

- [ ] **Step 2: Implement the low-priority high-order drain**

Add a separate worker drain that:
- claims from `session_metric_high_order_deferred_rebuild_queue`
- takes the same project lease/fence protections as other deferred drains
- rebuilds only `session_metric_buckets_dim2`/`dim3`/`dim4`
- runs after the readiness-critical drains

- [ ] **Step 3: Remove temporary isolation wiring**

Delete the temporary high-order toggle from worker config, env parsing, and benchmark wiring so the permanent architecture is the default behavior rather than an optional benchmark mode.

- [ ] **Step 4: Keep correctness-heavy paths full-scope**

Leave repair, range-delete, and purge paths rebuilding full session metric coverage synchronously or conservatively so those paths still restore all tiers before reactivation.

- [ ] **Step 5: Run the focused worker tests**

Run:
- `./scripts/docker-test.sh -p fantasma-worker --lib primary_session_metric_drain_enqueues_high_order_followup --quiet`
- `./scripts/docker-test.sh -p fantasma-worker --lib high_order_session_metric_drain_rebuilds_dim2_through_dim4 --quiet`

Expected: PASS

## Chunk 3: Docs, Full Verification, And Benchmark

### Task 5: Align docs and contracts

**Files:**
- Modify: `docs/architecture.md`
- Modify: `schemas/openapi/fantasma.yaml`
- Modify: `docs/STATUS.md`

- [ ] **Step 1: Update docs**

Describe the new freshness model plainly:
- all derived metrics remain eventually consistent
- session totals and 1D grouped reads are on the main deferred path
- higher-order session groupings may lag longer on a lower-priority lane

- [ ] **Step 2: Record status**

Add both the in-progress note and the completion note with the benchmark result and remaining open performance gap.

### Task 6: Run full verification and benchmark proof

**Files:**
- Modify: `crates/fantasma-bench/src/main.rs`

- [ ] **Step 1: Run repo-required verification**

Run:
- `cargo fmt --all --check`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo test -p fantasma-bench --quiet`
- `./scripts/docker-test.sh -p fantasma-worker --test pipeline --quiet`

- [ ] **Step 2: Rerun the representative local benchmark**

Run the same local SLO benchmark without temporary disable flags and save the artifact under a new March 31 directory for the permanent split.

- [ ] **Step 3: Compare against prior March 31 evidence**

Summarize how the permanent split compares to:
- baseline deferred-projection regression
- bitmap-decoupled run
- session-metric-high-order-disabled isolation run

- [ ] **Step 4: Commit**

```bash
git add crates/fantasma-store/migrations/0023_deferred_projection_rebuild_queues.sql \
  crates/fantasma-store/src/lib.rs \
  crates/fantasma-worker/src/worker.rs \
  crates/fantasma-worker/src/scheduler.rs \
  crates/fantasma-worker/src/main.rs \
  crates/fantasma-bench/src/main.rs \
  docs/architecture.md \
  schemas/openapi/fantasma.yaml \
  docs/STATUS.md \
  docs/superpowers/plans/2026-03-31-session-metric-high-order-lane-split.md
git commit -m "split high-order session metrics into a lower-priority lane"
```
