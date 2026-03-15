# Realistic Benchmark Reset Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rebuild the SLO benchmark harness so worker-facing performance gates reflect realistic live mobile upload traffic instead of giant historical windows.

**Architecture:** Replace the current window-first SLO scenario model with suite/scenario families for representative traffic, mixed repair, stress, and read visibility. Preserve the existing large-window workloads as stress coverage while introducing a deterministic upload schedule and sampled checkpoint readiness for the new representative worker gate.

**Tech Stack:** Rust, Tokio, Reqwest, Serde, Docker Compose, Markdown docs

---

### Task 1: Record The Reset And Lock Test Intent

**Files:**
- Modify: `docs/STATUS.md`
- Test: `crates/fantasma-bench/src/main.rs`

- [ ] Add an active-work `docs/STATUS.md` entry for the benchmark reset.
- [ ] Write failing tests for the new suite taxonomy, default suite selection, explicit scenario naming, repetition defaults, and publication grouping.
- [ ] Run the targeted `fantasma-bench` tests and confirm they fail for the old window-first model.

### Task 2: Replace The SLO Suite Model

**Files:**
- Modify: `crates/fantasma-bench/src/main.rs`
- Test: `crates/fantasma-bench/src/main.rs`

- [ ] Replace `append/backfill/repair/reads` window-first SLO definitions with `representative`, `mixed-repair`, `stress`, and `reads-visibility`.
- [ ] Keep legacy large-window workloads available under `stress-*` scenario keys instead of removing them.
- [ ] Add CLI suite selection, scenario filtering, repetition defaults, and publication metadata for the new suite model.
- [ ] Make both representative scenarios and both mixed-repair scenarios required gate scenarios in default runs.

### Task 3: Add Representative Upload Scheduling And Checkpoint Freshness

**Files:**
- Modify: `crates/fantasma-bench/src/main.rs`
- Test: `crates/fantasma-bench/src/main.rs`

- [ ] Add a deterministic schedule-driven workload generator for the representative and mixed-repair families.
- [ ] Preserve the current scale baseline while changing the upload shape to many small blobs and bounded medium offline flushes.
- [ ] Add sampled checkpoint readiness with the explicit default of every 20th normal append blob, plus all repair and offline-flush blobs.
- [ ] Publish checkpoint summary metrics in the main result and detailed checkpoint samples in sidecar artifacts.
- [ ] Keep Stage B attribution sidecars for representative, mixed-repair, and stress scenario outputs.

### Task 4: Update Docs And Project Memory

**Files:**
- Modify: `docs/performance.md`
- Modify: `docs/STATUS.md`

- [ ] Rewrite `docs/performance.md` around the new suite taxonomy, worker gate, separate read visibility suite, and stress reclassification.
- [ ] Add a short legacy mapping note from `append/backfill/repair-30d/90d/180d` to the new stress taxonomy.
- [ ] Update `docs/STATUS.md` at completion with the final benchmark-reset outcome and verification commands.

### Task 5: Verify The Slice

**Files:**
- Modify: `crates/fantasma-bench/src/main.rs`
- Modify: `docs/performance.md`
- Modify: `docs/STATUS.md`

- [ ] Run `cargo test -p fantasma-bench --quiet`.
- [ ] Run focused `cargo run -p fantasma-bench -- slo` commands covering one representative scenario, one mixed-repair scenario, one stress scenario, and one reads-visibility scenario.
- [ ] Run `docker compose -f infra/docker/compose.bench.yaml config`.
- [ ] Confirm results before reporting completion.
