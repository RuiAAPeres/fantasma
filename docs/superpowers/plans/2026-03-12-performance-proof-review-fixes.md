# Performance Proof Review Fixes Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix the post-review blockers in the performance-proof branch so benchmark runs are isolated, cleanup fails closed, the worker catch-up regression cannot pass vacuously, and the docs point at real verification commands.

**Architecture:** Keep the benchmark harness simple and explicit. The benchmark compose stack gets its own compose project and host ports so local runs cannot collide with the default stack, the harness startup path treats cleanup as a required precondition instead of best-effort behavior, and the pipeline regression test widens and proves the concurrent-read window directly.

**Tech Stack:** Rust, Tokio, Docker Compose, sqlx, Markdown docs.

---

### Task 1: Record The Review-Fix Slice

**Files:**
- Modify: `docs/STATUS.md`
- Create: `docs/superpowers/plans/2026-03-12-performance-proof-review-fixes.md`

- [ ] **Step 1: Mark the work active in project memory**
- [ ] **Step 2: Save this implementation plan under `docs/superpowers/plans/`**

### Task 2: Add Red Coverage For Benchmark Stack Safety

**Files:**
- Modify: `crates/fantasma-bench/src/main.rs`

- [ ] **Step 1: Add a failing unit test that asserts benchmark compose commands are scoped to a dedicated compose project**
- [ ] **Step 2: Add a failing unit test that asserts stack startup aborts if the cleanup command fails before `up` runs**
- [ ] **Step 3: Run the targeted `fantasma-bench` tests to watch them fail for the expected reasons**

### Task 3: Fix The Harness And Harden The Regression Test

**Files:**
- Modify: `crates/fantasma-bench/src/main.rs`
- Modify: `infra/docker/compose.bench.yaml`
- Modify: `crates/fantasma-worker/tests/pipeline.rs`

- [ ] **Step 1: Implement dedicated compose-project scoping plus isolated benchmark host ports**
- [ ] **Step 2: Make benchmark startup require successful cleanup before bringing the stack up**
- [ ] **Step 3: Update the worker catch-up regression so it proves at least one API request ran while the worker was still processing**
- [ ] **Step 4: Run focused tests again and keep changes minimal until they pass**

### Task 4: Align Docs And Project Memory

**Files:**
- Modify: `docs/performance.md`
- Modify: `docs/deployment.md`
- Modify: `docs/STATUS.md`

- [ ] **Step 1: Correct the documented worker test command**
- [ ] **Step 2: Document that the benchmark stack uses isolated ports/project state**
- [ ] **Step 3: Move the work item from `Active` to `Completed` in `docs/STATUS.md` with the concrete fixes shipped**

### Task 5: Verify Before Claiming Completion

**Files:**
- Verify only

- [ ] **Step 1: Run the focused `cargo test` commands for `fantasma-bench` and the hardened pipeline regression**
- [ ] **Step 2: Run `docker compose -f infra/docker/compose.bench.yaml config`**
- [ ] **Step 3: Run format checks on touched files and only then report completion**
