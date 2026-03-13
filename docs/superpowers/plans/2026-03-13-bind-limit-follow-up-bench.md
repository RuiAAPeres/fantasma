# Bind Limit Follow-Up Benchmark Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the remaining validation gaps by adding a cross-chunk conflict regression for chunked upserts and publishing a fresh end-to-end repair SLO run after the session-lane bind-limit follow-up.

**Architecture:** First add one focused store regression that proves `ON CONFLICT` accumulation still works when identical keys are split across chunk boundaries in the chunked path. Then rerun the repair SLO scenarios through `fantasma-bench`, publish fresh artifacts, and update docs so the evidence trail includes both store-level bind-limit coverage and a post-follow-up repair benchmark.

**Tech Stack:** Rust, `sqlx`, Postgres, Docker-backed Rust tests, `fantasma-bench`

---

## Chunk 1: Project Memory And Regression Coverage

### Task 1: Record the active follow-up

**Files:**
- Modify: `docs/STATUS.md`

- [x] Add an `Active` note for the cross-chunk conflict regression and fresh repair benchmark follow-up.

### Task 2: Add the cross-chunk conflict regression

**Files:**
- Modify: `crates/fantasma-store/src/lib.rs`
- Test: `crates/fantasma-store/src/lib.rs`

- [x] Write a focused store regression that forces repeated conflict keys to span multiple bind-budget chunks.
- [x] Run the focused test to verify whether the current chunked implementation already preserves accumulation semantics.

## Chunk 2: Fresh Repair Publication

### Task 3: Publish a new repair SLO run

**Files:**
- Create: `artifacts/performance/2026-03-13-repair-bind-follow-up/`

- [x] Run `fantasma-bench` for `repair-30d`, `repair-90d`, and `repair-180d` into a fresh output directory.
- [x] Inspect the resulting `summary.json` and scenario outputs for the published readiness numbers.

## Chunk 3: Documentation And Verification

### Task 4: Update docs with the new evidence

**Files:**
- Modify: `docs/STATUS.md`
- Modify: `docs/performance.md`

- [x] Move the `Active` note into `Completed` with the new verification commands.
- [x] Update the performance follow-up section to include the fresh repair publication and the new cross-chunk regression coverage.
- [x] Run final verification on the focused regression plus the relevant library checks before handoff.
