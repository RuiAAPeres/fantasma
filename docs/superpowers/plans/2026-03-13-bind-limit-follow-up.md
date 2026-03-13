# Bind Limit Follow-Up Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the remaining Postgres/sqlx bind-limit gap by extending batched-upsert chunking to session metrics and locking the behavior down with regression tests for the affected event and session helpers.

**Architecture:** Keep the existing set-based `INSERT ... VALUES ... ON CONFLICT` path, but apply the same bind-budget chunking used for event metrics to the analogous session metric helpers. Add store-level regressions that exercise oversized batches for both the remaining event helper variants and the session metric helpers so future per-row bind-budget mistakes fail fast.

**Tech Stack:** Rust, `sqlx`, Postgres, Docker-backed Rust tests in `fantasma-store`

---

## Chunk 1: Project Memory And Red Tests

### Task 1: Record the active follow-up

**Files:**
- Modify: `docs/STATUS.md`

- [x] Add a short `Active` entry describing the bind-limit follow-up and the intended scope.

### Task 2: Add failing regressions for oversized batch helpers

**Files:**
- Modify: `crates/fantasma-store/src/lib.rs`
- Test: `crates/fantasma-store/src/lib.rs`

- [x] Write focused store tests for oversized event total/dim1/dim2 batches and oversized session total/dim1/dim2 batches.
- [x] Run the focused tests and confirm at least the session-side regression fails on the current code for the expected bind-limit reason.

## Chunk 2: Minimal Store Fix

### Task 3: Chunk session metric upserts by bind budget

**Files:**
- Modify: `crates/fantasma-store/src/lib.rs`

- [x] Apply the existing `max_rows_per_batched_statement(...)` helper to `upsert_session_metric_totals_in_tx`, `upsert_session_metric_dim1_in_tx`, and `upsert_session_metric_dim2_in_tx`.
- [x] Keep the current `ON CONFLICT` accumulation semantics unchanged.
- [x] Re-run the new focused regressions until they pass.

## Chunk 3: Verification And Docs

### Task 4: Re-verify the changed slice and update project memory

**Files:**
- Modify: `docs/STATUS.md`
- Modify: `docs/performance.md`

- [x] Run the relevant focused store tests plus library checks for `fantasma-store` and `fantasma-worker`.
- [x] Update `docs/STATUS.md` to move the follow-up from active work into completed work with the verification commands.
- [x] Update `docs/performance.md` so the session-lane bind-limit follow-up is reflected alongside the earlier event-lane note.
