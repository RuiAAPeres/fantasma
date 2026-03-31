# Session Metric High-Order Deferral Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Keep contract-critical session metric totals and dim1 buckets rebuilding on the deferred path while allowing dim2/dim3/dim4 session cuboids to be disabled for isolation benchmarking.

**Architecture:** Extend the existing temporary deferred-drain control surface with a dedicated high-order session metric switch. The session metric rebuild helper will always rebuild totals and dim1 when session metrics are enabled, and only rebuild dim2+ when the new high-order switch is enabled. Benchmark and worker tests will prove that total + dim1 survive with high-order disabled and that the harness can drive the new toggle end to end.

**Tech Stack:** Rust, Tokio, SQLx, Postgres, fantasma-worker, fantasma-bench.

---

## Chunk 1: Red Tests And Surface Wiring

### Task 1: Add worker red test for high-order session metric deferral

**Files:**
- Modify: `crates/fantasma-worker/src/worker.rs`
- Test: `crates/fantasma-worker/src/worker.rs`

- [ ] **Step 1: Write the failing test**
- [ ] **Step 2: Run the targeted Docker-backed worker test to verify it fails for the missing control**
- [ ] **Step 3: Keep the failure output for reference and do not implement yet**

### Task 2: Add bench CLI/config red coverage for the new switch

**Files:**
- Modify: `crates/fantasma-bench/src/main.rs`
- Test: `crates/fantasma-bench/src/main.rs`

- [ ] **Step 1: Add parse/render tests for a `--disable-session-metric-high-order-deferred-rebuilds` flag and env propagation**
- [ ] **Step 2: Run `cargo test -p fantasma-bench --quiet` to verify the new tests fail before implementation**

## Chunk 2: Minimal Implementation

### Task 3: Extend deferred drain control and worker config

**Files:**
- Modify: `crates/fantasma-worker/src/worker.rs`
- Modify: `crates/fantasma-worker/src/scheduler.rs`
- Modify: `crates/fantasma-worker/src/main.rs`

- [ ] **Step 1: Add a `session_metric_high_order` control with default `true`**
- [ ] **Step 2: Thread the new control through worker config and env parsing**
- [ ] **Step 3: Update session projection drain logic to pass the new control into session metric rebuilds**

### Task 4: Split session metric rebuild breadth

**Files:**
- Modify: `crates/fantasma-worker/src/worker.rs`

- [ ] **Step 1: Refactor `rebuild_session_metric_buckets_in_tx` so total + dim1 are always rebuilt when session metrics are enabled**
- [ ] **Step 2: Gate dim2/dim3/dim4 delete-and-rebuild work behind the new high-order control**
- [ ] **Step 3: Keep default behavior unchanged when the new control is `true`**

### Task 5: Extend bench harness control surface

**Files:**
- Modify: `crates/fantasma-bench/src/main.rs`

- [ ] **Step 1: Add the new CLI flag and worker config field**
- [ ] **Step 2: Propagate it to compose env rendering and summary output**
- [ ] **Step 3: Preserve existing shorthand behavior for the broader session metric toggle**

## Chunk 3: Verification And Benchmark Proof

### Task 6: Prove the new behavior locally

**Files:**
- Modify: `docs/STATUS.md`
- Output: `artifacts/performance/2026-03-31-session-metric-high-order-disabled/`

- [ ] **Step 1: Run the targeted worker and bench tests again and confirm green**
- [ ] **Step 2: Run `cargo fmt --all --check` and `cargo clippy --workspace --all-targets -- -D warnings`**
- [ ] **Step 3: Run `./scripts/docker-test.sh -p fantasma-worker --test pipeline --quiet`**
- [ ] **Step 4: Run the representative benchmark with high-order session metrics disabled**
- [ ] **Step 5: Record the artifact path, result, and next recommendation in `docs/STATUS.md`**
