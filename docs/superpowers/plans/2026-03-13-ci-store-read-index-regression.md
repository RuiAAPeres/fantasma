# CI Store Read Index Regression Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restore the `CI` workflow by repairing the `fantasma-store` bounded read-index regression surfaced by the dim4 event-metrics expansion.

**Architecture:** Keep the fix narrow and schema-focused. Preserve the bucketed event-metrics model, restore the dim2/dim3 later-dimension value indexes that the read path depends on, and tighten the regression tests so they assert the actual index contract rather than a looser bucket-table expectation.

**Tech Stack:** Rust, SQLx/Postgres migrations, GitHub Actions CI, Markdown project memory.

---

### Task 1: Capture The Failure And Current Project Memory

**Files:**
- Modify: `docs/STATUS.md`
- Test: `.github/workflows/ci.yml`

- [ ] **Step 1: Record the active CI investigation**

Add an `Active` note in `docs/STATUS.md` that the March 13, 2026 CI break is currently traced to `fantasma-store` bounded read-index tests failing after the dim4 migration work.

- [ ] **Step 2: Reproduce the exact failing signal**

Run: `gh run view 23073557113 --log-failed`
Expected: FAIL output from `cargo test --workspace --quiet` showing `event_metrics_dim2_reads_use_bounded_read_indexes` and `event_metrics_dim3_reads_use_bounded_read_indexes`.

### Task 2: Lock The Regression Down In Tests First

**Files:**
- Modify: `crates/fantasma-store/src/lib.rs`

- [ ] **Step 1: Tighten the dim2/dim3 read-plan assertions**

Update the failing `#[sqlx::test]` coverage so the dim2 query expects the dedicated `dim2_value` read index and the dim3 query expects the dedicated later-dimension value index that matches the filter shape.

- [ ] **Step 2: Verify the tests still fail for the right reason**

Run one of:
- `./scripts/docker-test.sh -p fantasma-store event_metrics_dim2_reads_use_bounded_read_indexes -- --nocapture`
- `DATABASE_URL=postgres://fantasma:fantasma@127.0.0.1:5432/fantasma cargo test -p fantasma-store event_metrics_dim2_reads_use_bounded_read_indexes -- --nocapture`

Expected: FAIL because the required dim2/dim3 later-value indexes do not exist yet.

### Task 3: Restore The Missing Bucket Read Indexes

**Files:**
- Modify: `crates/fantasma-store/migrations/0009_bucketed_metrics_api.sql`
- Modify: `crates/fantasma-store/src/lib.rs`

- [ ] **Step 1: Add the missing dim2/dim3 later-value bucket indexes**

Mirror the old bounded-read coverage on the canonical bucket tables by adding dedicated value-specific indexes for dim2 and dim3 reads.

- [ ] **Step 2: Update migration/index inventory coverage**

Extend the migration and index-existence tests so the repository locks in the restored dim2/dim3 bucket read indexes alongside the existing dim4 bucket indexes.

- [ ] **Step 3: Run the targeted regression tests**

Run: `DATABASE_URL=postgres://fantasma:fantasma@127.0.0.1:5432/fantasma cargo test -p fantasma-store event_metrics_dim2_reads_use_bounded_read_indexes event_metrics_dim3_reads_use_bounded_read_indexes -- --nocapture`
Expected: PASS with no sequential scans and the expected dedicated value indexes present in the plan.

### Task 4: Re-Verify The Affected CI Slice And Record Prevention Notes

**Files:**
- Modify: `docs/STATUS.md`

- [ ] **Step 1: Run the affected CI-scope checks**

Run:
- `cargo fmt --all --check`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `DATABASE_URL=postgres://fantasma:fantasma@127.0.0.1:5432/fantasma cargo test --workspace --quiet`

Expected: PASS for the workspace checks that correspond to the failing CI job.

- [ ] **Step 2: Record the completed repair and prevention note**

Update `docs/STATUS.md` with the root cause, exact fix, fresh verification commands, and the lesson that bucket-schema migrations must preserve or replace the bounded later-dimension read indexes before retiring older metric tables.
