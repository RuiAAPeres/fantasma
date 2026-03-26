# Phase 3 Rebuild From C27 Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rebuild the proven append-first session-lane win from `c27dd24` by applying the phase-3 coordinator-batched session apply architecture plus the phase-4 bind-limit hardening, while excluding later unrelated runtime follow-ups.

**Architecture:** Keep the accepted Hotspot 3 ingest lease shape from `c27dd24`, preserve coordinator-held project fencing and replay semantics, and replace one-child-transaction-per-install incremental session apply with concurrent in-memory planning plus one coordinator-owned append transaction per worker batch. Carry forward the phase-4 lesson that wider batched writers must chunk at the Postgres bind limit, but do not pull in the later pool-scoped rebuild drain locks or other extra runtime changes that were bundled into `c385b1e`.

**Tech Stack:** Rust, Tokio, SQLx, Postgres, Docker-backed worker/store tests, `fantasma-worker`, `fantasma-store`, `fantasma-bench`

---

## File Map

- Modify: `crates/fantasma-worker/src/worker.rs`
  Rebuild the incremental session lane around plan-only concurrency plus one coordinator-owned batch apply transaction, and keep replay/failure semantics covered by focused worker tests.
- Modify: `crates/fantasma-store/src/lib.rs`
  Add the batch helpers required by the coordinator-owned session apply path and chunk the widened batched writers so larger worker batch sizes do not hit Postgres bind cliffs.
- Modify: `docs/STATUS.md`
  Record the rebuild start, verification, benchmark outcome, and keep/backout decision.
- Output: `artifacts/performance/2026-03-25-phase3-rebuild-from-c27/`
  Representative append benchmark evidence for the rebuilt phase-3/4 shape.

## Chunk 1: Lock The Clean Scope With Tests

### Task 1: Add worker regressions for coordinator-owned batched apply

**Files:**
- Modify: `crates/fantasma-worker/src/worker.rs`

- [ ] **Step 1: Add a source-shape regression for the worker flow**

Add a focused worker test that inspects the session batch helper region and asserts:
- incremental work items still derive install-local plans with `plan_incremental_session_batch(...)`
- the coordinator calls `apply_incremental_session_plans_in_tx(...)`
- the worker no longer uses one child transaction per install for incremental apply

- [ ] **Step 2: Run the focused worker regression and verify it fails**

Run:
```bash
cargo test -p fantasma-worker --lib worker::tests::session_batch_uses_one_batched_incremental_apply -- --exact
```

Expected:
- FAIL because the `c27dd24` worker still commits incremental apply per install

### Task 2: Add failing store regressions for the batch helper surface

**Files:**
- Modify: `crates/fantasma-store/src/lib.rs`

- [ ] **Step 1: Add a failing `install_first_seen` batch-helper test**

Require a new helper that inserts many first-seen rows, dedupes duplicate input installs, and reports only the newly inserted install ids.

- [ ] **Step 2: Add a failing batched tail-update test**

Require a helper that updates multiple tail sessions and reports the exact row count updated.

- [ ] **Step 3: Add a failing batched install-session-state upsert test**

Require a helper that upserts many install session states and updates existing rows in place.

- [ ] **Step 4: Add failing bind-limit regressions for widened batch writers**

Require above-bind-limit success for:
- `insert_sessions_in_tx`
- `upsert_session_daily_install_deltas_in_tx`
- `upsert_session_daily_deltas_in_tx`
- `add_session_daily_duration_deltas_in_tx`

- [ ] **Step 5: Run the focused store regressions and verify they fail**

Run:
```bash
FANTASMA_DOCKER_TEST_KEEP_CACHE=1 ./scripts/docker-test.sh -p fantasma-store tests::batch_insert_install_first_seen_inserts_only_new_rows --lib -- --exact
FANTASMA_DOCKER_TEST_KEEP_CACHE=1 ./scripts/docker-test.sh -p fantasma-store tests::batch_update_session_tails_updates_all_requested_rows --lib -- --exact
FANTASMA_DOCKER_TEST_KEEP_CACHE=1 ./scripts/docker-test.sh -p fantasma-store tests::batch_upsert_install_session_states_updates_existing_rows --lib -- --exact
FANTASMA_DOCKER_TEST_KEEP_CACHE=1 ./scripts/docker-test.sh -p fantasma-store --lib handle_batches_above_postgres_bind_limit -- --nocapture
```

Expected:
- FAIL because the batch helpers and bind-limit hardening do not exist yet

## Chunk 2: Rebuild The Store Batch Helpers And Batched Writers

### Task 3: Add the batch helpers used by coordinator-owned apply

**Files:**
- Modify: `crates/fantasma-store/src/lib.rs`

- [ ] **Step 1: Implement `insert_install_first_seen_many_in_tx(...)`**

Use chunked `INSERT ... ON CONFLICT DO NOTHING RETURNING install_id` batches, dedupe duplicate input installs, and return only newly inserted install ids.

- [ ] **Step 2: Implement `update_session_tails_in_tx(...)`**

Batch multiple tail updates with a `WITH input(...)` / `VALUES` shape and return total updated rows.

- [ ] **Step 3: Implement `upsert_install_session_states_in_tx(...)`**

Batch install-session-state upserts and preserve the current row semantics.

### Task 4: Apply phase-4 bind-limit hardening to widened session writers

**Files:**
- Modify: `crates/fantasma-store/src/lib.rs`

- [ ] **Step 1: Chunk `insert_sessions_in_tx(...)`**
- [ ] **Step 2: Chunk `upsert_session_daily_install_deltas_in_tx(...)` and keep merged active-install deltas correct**
- [ ] **Step 3: Chunk `upsert_session_daily_deltas_in_tx(...)`**
- [ ] **Step 4: Chunk `add_session_daily_duration_deltas_in_tx(...)`**

- [ ] **Step 5: Run the focused store regressions**

Run:
```bash
FANTASMA_DOCKER_TEST_KEEP_CACHE=1 ./scripts/docker-test.sh -p fantasma-store tests::batch_insert_install_first_seen_inserts_only_new_rows --lib -- --exact
FANTASMA_DOCKER_TEST_KEEP_CACHE=1 ./scripts/docker-test.sh -p fantasma-store tests::batch_update_session_tails_updates_all_requested_rows --lib -- --exact
FANTASMA_DOCKER_TEST_KEEP_CACHE=1 ./scripts/docker-test.sh -p fantasma-store tests::batch_upsert_install_session_states_updates_existing_rows --lib -- --exact
FANTASMA_DOCKER_TEST_KEEP_CACHE=1 ./scripts/docker-test.sh -p fantasma-store --lib handle_batches_above_postgres_bind_limit -- --nocapture
```

Expected:
- all focused store tests pass

## Chunk 3: Rebuild The Worker Around One Coordinator-Owned Apply

### Task 5: Replace per-install incremental apply with concurrent planning plus one append commit

**Files:**
- Modify: `crates/fantasma-worker/src/worker.rs`
- Modify: `crates/fantasma-store/src/lib.rs`

- [ ] **Step 1: Refactor the incremental queue into plan-only work**

Change the queue so it:
- derives `IncrementalSessionPlan`s concurrently in memory
- preserves stable output ordering
- no longer opens per-install child transactions or per-install `session_apply` leases

- [ ] **Step 2: Add `apply_incremental_session_plans_in_tx(...)`**

Merge many completed plans into one coordinator-owned append transaction that:
- batches first-seen rows by project
- batches tail updates
- inserts all new sessions
- batches daily install deltas, daily row upserts, and duration-only updates
- merges metric accumulators by project before writing the existing session metric tables
- enqueues touched bitmap rebuild days
- batches install-session-state upserts

- [ ] **Step 3: Preserve replay and coordinator-failure semantics**

Keep the current behavior where:
- append writes can survive coordinator finalize failure
- `worker_offsets` advances only in the coordinator finalize transaction
- replay filters already-applied append work through install/session state, not through a prematurely moved offset

- [ ] **Step 4: Do not reintroduce unrelated extras**

Keep the rebuild scoped to phase-3/4 only:
- no pool-scoped rebuild drain lock maps
- no bitmap enqueue batching follow-up
- no metric fusion follow-up

- [ ] **Step 5: Run focused worker coverage**

Run:
```bash
cargo test -p fantasma-worker --lib worker::tests::session_batch_uses_one_batched_incremental_apply -- --exact
FANTASMA_DOCKER_TEST_KEEP_CACHE=1 ./scripts/docker-test.sh -p fantasma-worker worker::tests::replayed_append_batch_only_advances_offset_after_coordinator_failure --lib -- --exact
FANTASMA_DOCKER_TEST_KEEP_CACHE=1 ./scripts/docker-test.sh -p fantasma-worker worker::tests::cross_midnight_append_commit_leaves_daily_and_metric_state_final --lib -- --exact
```

Expected:
- the new worker regression passes
- existing replay/finality regressions still pass

## Chunk 4: Verify, Benchmark, Review, And Record

### Task 6: Run the required proof and benchmark the rebuilt shape

**Files:**
- Modify: `docs/STATUS.md`

- [ ] **Step 1: Record the rebuild start in project memory**

Add a `docs/STATUS.md` entry describing the scope: rebuild phase-3/4 from `c27dd24`, keep the worker/store changes benchmark-owned, and exclude later unrelated runtime additions.

- [ ] **Step 2: Run the required verification**

Run:
```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
FANTASMA_DOCKER_TEST_KEEP_CACHE=1 ./scripts/docker-test.sh -p fantasma-store --lib --quiet
FANTASMA_DOCKER_TEST_KEEP_CACHE=1 ./scripts/docker-test.sh -p fantasma-worker --test pipeline --quiet
git diff --check
```

Expected:
- all commands exit 0

- [ ] **Step 3: Run the representative append benchmark pair**

Run:
```bash
FANTASMA_ADMIN_TOKEN=test-token cargo run -p fantasma-bench -- slo --output-dir artifacts/performance/2026-03-25-phase3-rebuild-from-c27 --scenario live-append-small-blobs --scenario live-append-plus-light-repair
```

Expected:
- `live-append-small-blobs` append time improves or stays flat versus Hotspot 3 `130623ms`
- `live-append-plus-light-repair` append time improves or stays flat versus Hotspot 3 `122770ms`
- checkpoint/final derived readiness is not materially worse

- [ ] **Step 4: Run subagent review on the implementation**

Dispatch:
- one spec-compliance review against this plan
- one code-quality review focused on worker/store correctness, batching semantics, and bind-limit safety

Address any findings before accepting the rebuild.

- [ ] **Step 5: Record the measured result in `docs/STATUS.md`**

Capture:
- verification commands run
- exact benchmark deltas versus Hotspot 3
- whether the rebuilt phase-3/4 shape is accepted or backed out
