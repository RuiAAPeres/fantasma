# Deferred Drain Isolation Benchmark Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add temporary benchmark-only worker switches that let us disable deferred drain families one at a time, then rerun the local representative benchmark to isolate which family is starving append throughput.

**Architecture:** Thread three boolean drain toggles through the worker config and benchmark harness, defaulting to current behavior so production semantics stay unchanged. Use one focused regression to prove each switch suppresses only its targeted deferred drain calls, then run the benchmark variants in the agreed order and compare the attribution artifacts.

**Tech Stack:** Rust, Tokio, SQLx/Postgres, existing `fantasma-bench` Docker harness

---

## Chunk 1: Worker and Harness Wiring

### Task 1: Add worker config support for deferred-drain isolation

**Files:**
- Modify: `crates/fantasma-worker/src/scheduler.rs`
- Modify: `crates/fantasma-worker/src/main.rs`
- Modify: `crates/fantasma-worker/src/worker.rs`

- [ ] **Step 1: Add config fields for drain toggles**
- [ ] **Step 2: Parse worker env vars into those fields in `main.rs`**
- [ ] **Step 3: Gate each deferred drain call site in `worker.rs` with the matching toggle**
- [ ] **Step 4: Keep defaults equivalent to current behavior**

### Task 2: Expose the toggles through the benchmark harness

**Files:**
- Modify: `crates/fantasma-bench/src/main.rs`

- [ ] **Step 1: Extend `BenchWorkerConfig` with the same toggle fields**
- [ ] **Step 2: Add CLI flags for the isolation switches**
- [ ] **Step 3: Render the corresponding worker env vars into the generated benchmark compose file**
- [ ] **Step 4: Include the toggles in benchmark run metadata so artifacts are self-describing**

## Chunk 2: Regression Proof

### Task 3: Add focused tests for the switches

**Files:**
- Modify: `crates/fantasma-worker/src/worker.rs`
- Modify: `crates/fantasma-bench/src/main.rs`

- [ ] **Step 1: Write a failing worker test that proves a disabled deferred family is not drained while other work still advances**
- [ ] **Step 2: Run the targeted worker test and confirm it fails**
- [ ] **Step 3: Implement the minimal code to make it pass**
- [ ] **Step 4: Add or update a bench test covering env rendering / CLI parsing for the new flags**
- [ ] **Step 5: Run the targeted tests and confirm they pass**

## Chunk 3: Verification and Benchmarks

### Task 4: Verify the code slices before benchmarking

**Files:**
- Modify: `docs/STATUS.md`

- [ ] **Step 1: Run `cargo fmt --all --check`**
- [ ] **Step 2: Run `cargo clippy --workspace --all-targets -- -D warnings`**
- [ ] **Step 3: Run targeted `fantasma-worker` tests for the new switches**
- [ ] **Step 4: Run targeted `fantasma-bench` tests for CLI / compose rendering**

### Task 5: Run the isolation benchmark sequence

**Files:**
- Modify: `docs/STATUS.md`

- [ ] **Step 1: Run the representative local benchmark with `install_activity` drains disabled**
- [ ] **Step 2: Run the representative local benchmark with `session_projection` drains disabled**
- [ ] **Step 3: If needed, run the representative local benchmark with `event_metric` drains disabled**
- [ ] **Step 4: Compare `append_uploads`, `upload_to_session_lane_ready` p95, and `latest_lane_to_derived_visible` p95 across runs**
- [ ] **Step 5: Record the findings and likely dominant offender in `docs/STATUS.md`**
