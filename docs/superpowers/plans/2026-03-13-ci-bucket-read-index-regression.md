# CI Bucket Read-Index Regression Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restore the `CI` workflow by fixing the dim2/dim3 event-metric bucket read-index regression surfaced by `cargo test --workspace --quiet`.

**Architecture:** Keep the repair narrow and consistent with the dim4 design that just landed. First lock the expected dim2/dim3 value-filter access paths in failing store tests, then add a forward migration that gives the dim2/dim3 bucket tables the same key-aware plus value-specific read indexes as dim4, and finally re-run the CI-equivalent checks and record the prevention note in project memory.

**Tech Stack:** Rust, sqlx/Postgres migrations, Cargo workspace tests, Markdown project memory.

---

### Task 1: Lock The Regression In Failing Tests

**Files:**
- Modify: `crates/fantasma-store/src/lib.rs`
- Test: `crates/fantasma-store/src/lib.rs`

- [ ] Update the dim2 and dim3 read-plan tests so they assert the same value-specific index-selection pattern already used by the dim4 test.
- [ ] Run `./scripts/docker-test.sh -p fantasma-store event_metrics_dim -- --nocapture`
- [ ] Confirm the updated tests still fail before any migration changes because the required dim2/dim3 indexes do not exist yet.

### Task 2: Add The Missing Bucket Read Indexes

**Files:**
- Modify: `crates/fantasma-store/migrations/0012_event_metric_dim4_cleanup.sql`
- Modify: `crates/fantasma-store/src/lib.rs`

- [ ] Extend the forward migration so `event_metric_buckets_dim2` and `event_metric_buckets_dim3` gain key-aware base indexes plus per-dimension value indexes that preserve bounded later-dimension filters.
- [ ] Update the migration inventory test to expect the new dim2/dim3 bucket indexes alongside the existing dim4 set.

### Task 3: Re-Verify The CI Slice And Record The Lesson

**Files:**
- Modify: `docs/STATUS.md`

- [ ] Run `cargo fmt --all --check`
- [ ] Run `./scripts/docker-test.sh -p fantasma-store event_metrics_dim -- --nocapture`
- [ ] Run `./scripts/docker-test.sh --quiet`
- [ ] Record the completed repair, verification evidence, and the prevention note about keeping lower-arity bucket indexes aligned whenever a new bucket arity is added.
