# Performance Proofing Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prove Fantasma's bounded performance claims with deterministic guardrail tests in PR CI and numeric end-to-end benchmark runs on the Docker stack.

**Architecture:** Add deterministic tests around worker write amplification, exact-day repair scope, and indexed event-metrics reads directly in the Rust workspace. Add a dedicated `fantasma-bench` runner that boots a benchmark-specific Docker stack, drives fixed workloads through the public ingest and metrics APIs, emits JSON summaries, and is enforced only on `main` and manual benchmark runs.

**Tech Stack:** Rust, Tokio, sqlx, Docker Compose, GitHub Actions, Markdown docs

---

## Chunk 1: Project Memory And Deterministic Guardrails

### Task 1: Save project memory and lock the proof surface

**Files:**
- Modify: `docs/STATUS.md`
- Create: `docs/superpowers/plans/2026-03-12-performance-proofing.md`

- [ ] Record this performance-proofing slice in `docs/STATUS.md` as active work.
- [ ] Save the approved implementation plan in `docs/superpowers/plans/`.

### Task 2: Add deterministic worker and query-plan guardrails

**Files:**
- Modify: `crates/fantasma-worker/src/worker.rs`
- Modify: `crates/fantasma-store/src/lib.rs`

- [ ] Add a failing regression that proves the max-dimension event payload still expands into the bounded rollup fanout implied by the current built-in dimensions plus the 3-property cap.
- [ ] Add a failing regression that proves late-event repair still rewrites only the touched UTC start days under a realistic non-contiguous workload.
- [ ] Add failing store-level `EXPLAIN (FORMAT JSON)` tests for representative dim2 and dim3 event-metrics read queries and assert the planner stays on the bounded read indexes rather than seq scanning the cube tables.
- [ ] Run the targeted tests, confirm they fail for the expected reasons, then implement the minimum code or test helpers needed so they pass.

## Chunk 2: Numeric Stack Benchmark Harness

### Task 3: Add a first-class Rust benchmark runner and benchmark stack

**Files:**
- Modify: `Cargo.toml`
- Create: `crates/fantasma-bench/Cargo.toml`
- Create: `crates/fantasma-bench/src/main.rs`
- Create: `infra/docker/compose.bench.yaml`
- Create: `docs/performance.md`

- [ ] Add a `fantasma-bench` workspace member with a CLI that runs fixed `hot-path` and `repair-path` stack scenarios.
- [ ] Drive the public Docker stack through `POST /v1/events` and the existing metrics routes, measuring ingest throughput, derive lag, and warmed query latency.
- [ ] Emit machine-readable JSON output and a short Markdown summary per run.
- [ ] Add a benchmark-specific Compose file that keeps the same service topology but lowers the worker poll interval for measurement.
- [ ] Commit a checked-in `ci` budget file for both scenarios.

## Chunk 3: CI And Documentation

### Task 4: Wire the benchmark layer into automation and docs

**Files:**
- Modify: `.github/workflows/ci.yml`
- Create: `.github/workflows/performance.yml`
- Modify: `docs/deployment.md`
- Modify: `docs/STATUS.md`

- [ ] Keep PR CI on deterministic guardrails only.
- [ ] Add a dedicated performance workflow that runs on pushes to `main` and `workflow_dispatch`, uploads JSON artifacts, and fails when the committed `ci` budgets are exceeded.
- [ ] Document the benchmark commands, metrics, budget model, and benchmark-only Docker overrides in `docs/performance.md`, with a short reference from deployment docs.
- [ ] Update `docs/STATUS.md` when the slice ships.

## Chunk 4: Verification

### Task 5: Verify the complete slice

**Files:**
- Modify: `docs/STATUS.md`

- [ ] Run targeted Rust tests for the new deterministic guardrails.
- [ ] Run `cargo fmt --all --check`.
- [ ] Run `cargo clippy --workspace --all-targets -- -D warnings`.
- [ ] Run `cargo test --workspace --quiet`.
- [ ] Run the benchmark harness locally against `infra/docker/compose.bench.yaml` for both scenarios and capture fresh outputs.
- [ ] Record the landed slice in `docs/STATUS.md`.
