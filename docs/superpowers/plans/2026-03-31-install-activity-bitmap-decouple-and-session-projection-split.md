# Install-Activity Bitmap Decouple And Session-Projection Split Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop install-activity drains from rebuilding active-install bitmaps inline, split session-projection isolation between `session_daily` and `session_metric_buckets_*`, and rerun the local representative benchmark after each split.

**Architecture:** Keep install-activity rebuilds per-install and make bitmap work fully queue-driven by coalescing touched days into the existing bitmap queue. Extend the temporary benchmark toggles so session-projection drains can independently disable `session_daily` rebuilds and session metric bucket rebuilds without changing default production behavior.

**Tech Stack:** Rust, Tokio, SQLx/Postgres, existing `fantasma-worker` and `fantasma-bench` Docker benchmark harness

---

## Chunk 1: Install-Activity Drain Shape

### Task 1: Add failing tests for bitmap decoupling

**Files:**
- Modify: `crates/fantasma-worker/src/worker.rs`

- [ ] **Step 1: Write a failing worker regression proving install-activity drain enqueues touched bitmap days instead of rebuilding bitmaps inline**
- [ ] **Step 2: Run the targeted worker regression and confirm it fails for the current inline rebuild shape**

### Task 2: Make install-activity drains queue bitmap work instead of rebuilding inline

**Files:**
- Modify: `crates/fantasma-worker/src/worker.rs`

- [ ] **Step 1: Replace inline bitmap rebuilds inside `drain_pending_install_activity_deferred_rebuilds` with bitmap-day queue enqueueing**
- [ ] **Step 2: Keep the drain transaction atomic so touched install state and touched bitmap-day queue rows commit together**
- [ ] **Step 3: Run the targeted worker regression and confirm it passes**

## Chunk 2: Split Session-Projection Isolation

### Task 3: Add failing tests for split session-projection controls

**Files:**
- Modify: `crates/fantasma-worker/src/worker.rs`
- Modify: `crates/fantasma-bench/src/main.rs`
- Modify: `crates/fantasma-worker/src/scheduler.rs`
- Modify: `crates/fantasma-worker/src/main.rs`

- [ ] **Step 1: Write a failing worker regression proving `session_daily` rebuilds can be suppressed without suppressing session metric bucket rebuilds**
- [ ] **Step 2: Write a failing worker regression proving session metric bucket rebuilds can be suppressed independently**
- [ ] **Step 3: Add or update bench CLI/rendering tests for the new split toggles**
- [ ] **Step 4: Run the targeted tests and confirm they fail before implementation**

### Task 4: Implement the split controls

**Files:**
- Modify: `crates/fantasma-worker/src/worker.rs`
- Modify: `crates/fantasma-worker/src/scheduler.rs`
- Modify: `crates/fantasma-worker/src/main.rs`
- Modify: `crates/fantasma-bench/src/main.rs`

- [ ] **Step 1: Extend deferred-drain control with separate `session_daily` and session metric bucket switches**
- [ ] **Step 2: Thread the new env/config/CLI flags through worker startup and benchmark compose rendering**
- [ ] **Step 3: Gate `rebuild_session_daily_days_in_tx` and `rebuild_session_metric_buckets_in_tx` independently inside the session-projection drain**
- [ ] **Step 4: Run the targeted tests and confirm they pass**

## Chunk 3: Verification And Benchmarks

### Task 5: Re-verify the code slices

**Files:**
- Modify: `docs/STATUS.md`

- [ ] **Step 1: Run `cargo fmt --all --check`**
- [ ] **Step 2: Run `cargo test -p fantasma-bench --quiet`**
- [ ] **Step 3: Run targeted Docker-backed `fantasma-worker` regressions for bitmap decoupling and split session-projection toggles**
- [ ] **Step 4: Run `cargo clippy --workspace --all-targets -- -D warnings`**

### Task 6: Run the narrowed benchmark sequence

**Files:**
- Modify: `docs/STATUS.md`

- [ ] **Step 1: Run the representative local benchmark with install-activity drains decoupled but otherwise default**
- [ ] **Step 2: Run the representative local benchmark with only `session_daily` rebuilds disabled**
- [ ] **Step 3: Run the representative local benchmark with only session metric bucket rebuilds disabled**
- [ ] **Step 4: Compare append throughput and attribution artifacts against the baseline and the earlier install-activity-disable run**
- [ ] **Step 5: Record the dominant remaining offender and any benchmark-shape caveats in `docs/STATUS.md`**
