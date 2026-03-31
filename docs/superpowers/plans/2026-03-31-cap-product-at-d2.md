# D2 Product Cap Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Hard-cap Fantasma’s public product shape at max D2 for event and session metrics by removing D3/D4 storage, rebuilds, and public query support.

**Architecture:** Treat D2 as the full contract everywhere, not just the critical path. Event and session bucketed metrics stop at `total`/`dim1`/`dim2`; any higher-order queue, lane, SQL, table, and parser path is removed instead of merely deprioritized.

**Tech Stack:** Rust, Tokio, SQLx, Postgres migrations, Axum, Docker-backed worker tests

---

## File Map

- Modify: `crates/fantasma-api/src/http.rs`
  - Cap bucketed event/session query parsing and read-shape validation at D2, and update parser/contract tests.
- Modify: `crates/fantasma-store/migrations/0023_deferred_projection_rebuild_queues.sql`
  - Add forward drops for D3/D4 event/session bucket tables and remove the now-unused high-order session queue.
- Modify: `crates/fantasma-store/src/lib.rs`
  - Remove D3/D4 event/session bucket structs, upsert helpers, fetch helpers, migration-shape assertions, and queue helpers/tests that no longer apply.
- Modify: `crates/fantasma-worker/src/worker.rs`
  - Remove D3/D4 event/session rebuild logic, remove the session high-order lane/drain, and keep deferred/eventual rebuilds bounded at D2.
- Modify: `crates/fantasma-worker/src/lib.rs`
  - Stop exporting the removed high-order drain entrypoint.
- Modify: `crates/fantasma-worker/src/scheduler.rs`
  - Remove the dedicated `session_metric_high_order` lane and its connection budget.
- Modify: `crates/fantasma-worker/src/main.rs`
  - Remove any now-dead lane wiring.
- Modify: `crates/fantasma-worker/tests/pipeline.rs`
  - Replace any D3/D4 visibility expectations with D2-capped behavior.
- Modify: `docs/architecture.md`
  - State plainly that bucketed event/session metrics stop at D2.
- Modify: `schemas/openapi/fantasma.yaml`
  - Cap public query documentation at D2 and remove “higher-order may lag” wording.
- Modify: `docs/STATUS.md`
  - Record start and finish of the D2 cap follow-up.

## Chunk 1: Lock The D2 Contract In Tests

### Task 1: Add failing parser regressions for bucketed metrics

**Files:**
- Modify: `crates/fantasma-api/src/http.rs`

- [ ] **Step 1: Write the failing tests**

Replace the current “accepts four group_by dimensions” coverage with regressions that assert:
- event bucket metrics reject three or more referenced dimensions
- session bucket metrics reject three or more referenced dimensions
- D2 requests still parse for event/session bucket metrics

- [ ] **Step 2: Run the focused parser tests and watch them fail**

Run:
- `cargo test -p fantasma-api parse_event_metrics_query_accepts_four_group_by_dimensions -- --exact`
- `cargo test -p fantasma-api parse_session_metric_query_accepts_four_group_by_dimensions_for_bucket_metrics -- --exact`

Expected: FAIL after the test rewrite because the implementation still allows D3/D4.

### Task 2: Add a failing worker/store regression proving D2 is the ceiling

**Files:**
- Modify: `crates/fantasma-worker/src/worker.rs`
- Modify: `crates/fantasma-worker/tests/pipeline.rs`

- [ ] **Step 1: Write the failing tests**

Add or rewrite focused regressions so they assert:
- deferred session metric rebuilds populate at most `total`/`dim1`/`dim2`
- no session high-order follow-up queue or lane is required anymore
- representative pipeline reads still return D2 session/event grouped metrics after the deferred drains complete

- [ ] **Step 2: Run the focused worker tests and watch them fail**

Run:
- `./scripts/docker-test.sh -p fantasma-worker --lib primary_session_metric_drain_keeps_total_dim1_dim2_only --quiet`
- `./scripts/docker-test.sh -p fantasma-worker --test pipeline --quiet`

Expected: FAIL because worker/store still maintain D3/D4 paths.

## Chunk 2: Remove D3/D4 Storage And Rebuilds

### Task 3: Drop D3/D4 tables and helpers

**Files:**
- Modify: `crates/fantasma-store/migrations/0023_deferred_projection_rebuild_queues.sql`
- Modify: `crates/fantasma-store/src/lib.rs`

- [ ] **Step 1: Add the forward migration changes**

Extend the latest forward migration so a fresh stack lands on the D2 contract:
- drop `event_metric_buckets_dim3`
- drop `event_metric_buckets_dim4`
- drop `session_metric_buckets_dim3`
- drop `session_metric_buckets_dim4`
- drop `session_metric_high_order_deferred_rebuild_queue`

- [ ] **Step 2: Remove dead store code**

Delete:
- D3/D4 bucket delta structs and upsert helpers
- D3/D4 fetch helpers and query dispatch branches
- high-order session queue helpers and tests

- [ ] **Step 3: Run focused store verification**

Run:
- `cargo test -p fantasma-store forward_migration_keeps_event_bucket_storage_at_dim2_and_cleans_legacy_daily_tables -- --exact`
- `cargo test -p fantasma-store forward_migration_keeps_session_storage_at_dim2_and_adds_snapshot_columns -- --exact`

Expected: PASS

### Task 4: Remove worker high-order logic and cap rebuild SQL at D2

**Files:**
- Modify: `crates/fantasma-worker/src/worker.rs`
- Modify: `crates/fantasma-worker/src/lib.rs`
- Modify: `crates/fantasma-worker/src/scheduler.rs`
- Modify: `crates/fantasma-worker/src/main.rs`

- [ ] **Step 1: Remove D3/D4 rebuild SQL**

Trim event/session rebuild SQL and any incremental delta builders so they stop at `dim2`.

- [ ] **Step 2: Remove the session high-order lane**

Delete:
- the high-order queue usage
- the drain function/export
- the dedicated scheduler lane and extra connection budget

- [ ] **Step 3: Keep correctness-heavy paths aligned**

Repair, range-delete, and purge should now treat D2 as the full projection surface.

- [ ] **Step 4: Run focused worker verification**

Run:
- `./scripts/docker-test.sh -p fantasma-worker --lib --quiet`

Expected: PASS

## Chunk 3: Public Docs And Full Verification

### Task 5: Align docs and route descriptions

**Files:**
- Modify: `docs/architecture.md`
- Modify: `schemas/openapi/fantasma.yaml`
- Modify: `docs/STATUS.md`

- [ ] **Step 1: Update docs**

State that:
- bucketed event metrics stop at D2
- bucketed session metrics stop at D2
- no higher-order bucket support exists in the current product

- [ ] **Step 2: Record project memory**

Add STATUS start and finish notes with the D2 decision and any benchmark implications.

### Task 6: Run required verification

- [ ] **Step 1: Run repo-required checks**

Run:
- `cargo fmt --all --check`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `./scripts/verify-changed-surface.sh run api-contract`
- `./scripts/docker-test.sh -p fantasma-worker --lib --quiet`

- [ ] **Step 2: Commit**

```bash
git add crates/fantasma-api/src/http.rs \
  crates/fantasma-store/migrations/0023_deferred_projection_rebuild_queues.sql \
  crates/fantasma-store/src/lib.rs \
  crates/fantasma-worker/src/worker.rs \
  crates/fantasma-worker/src/lib.rs \
  crates/fantasma-worker/src/scheduler.rs \
  crates/fantasma-worker/src/main.rs \
  crates/fantasma-worker/tests/pipeline.rs \
  docs/architecture.md \
  schemas/openapi/fantasma.yaml \
  docs/STATUS.md \
  docs/superpowers/plans/2026-03-31-cap-product-at-d2.md
git commit -m "cap metric cubes at d2"
```
