# Session Append Write-Set Fusion Design

**Goal:** Reduce fixed Postgres statement overhead on the session append path without changing sessionization semantics, repair behavior, public APIs, or metric outputs.

## Problem

The accepted ingest-path work improved raw-event append throughput, but the session worker still pays a long fixed SQL chain for each incremental install batch in `crates/fantasma-worker/src/worker.rs`.

`apply_incremental_session_plan()` currently performs:

1. optional `insert_install_first_seen_in_tx()`
2. optional `update_session_tail_in_tx()`
3. `insert_sessions_in_tx()`
4. `upsert_session_active_install_slices_in_tx()`
5. `upsert_session_daily_install_deltas_in_tx()`
6. `upsert_session_daily_deltas_in_tx()`
7. `add_session_daily_duration_deltas_in_tx()`
8. `upsert_session_metric_totals_in_tx()`
9. `upsert_session_metric_dim1_in_tx()`
10. `upsert_session_metric_dim2_in_tx()`
11. `upsert_session_metric_dim3_in_tx()`
12. `upsert_session_metric_dim4_in_tx()`
13. `enqueue_touched_active_install_bitmap_rebuilds_in_tx()`
14. `upsert_install_session_state_in_tx()`

This path is correct, but it still spends a lot of fixed statement and planning overhead on the steady-state append path that SDK traffic exercises continuously.

The rejected March 25 Session D4 experiment already showed that moving append work behind rebuild/finalization is the wrong trade for this product. The next attempt must stay incremental and append-first.

## Constraints

- Keep public API, worker correctness, and aggregate semantics unchanged.
- Preserve the current incremental sessionization model:
  - `sessions` remains the source of truth for derived sessions
  - `install_first_seen` stays insert-once
  - tail-session extension behavior stays the same
  - repair and rebuild paths keep their current design
- Do not move session metrics behind the rebuild finalizer again.
- Do not add schema changes in this slice unless profiling proves they are necessary.
- Preserve existing invariant checks:
  - inserted session count must still match `plan.new_sessions.len()`
  - tail update must still affect exactly one existing row
  - duration-only day updates must still fail loudly if the corresponding `session_daily` row is unexpectedly missing
- Preserve current metric contents and deterministic output ordering.
- Keep transaction scope unchanged at the worker level; this slice is about fewer statements inside the existing transaction, not a different correctness model.

## Current Hot Path

The append plan already gives us the right logical write set:

- `first_seen`
- `tail_update`
- `new_sessions`
- `active_install_slices`
- `daily_install_deltas`
- `daily_session_counts_by_day`
- `daily_duration_deltas_by_day`
- `session_metric_accumulator`
- `active_install_bitmap_days`
- `next_state`

The cost problem is not missing information. It is that the store layer executes the logical write set as several narrowly-scoped statements, including:

- one statement for `session_daily_installs`
- one statement for `session_daily` upserts
- one statement for duration-only `session_daily` updates
- five independent statements for the five `session_metric_*` cuboid tables

That is the best place to squeeze more append throughput without reopening sessionization or rebuild design.

## Approaches Considered

### 1. Recommended: behavior-preserving store fusion

Keep worker planning unchanged, but replace the repetitive store helper chain with two fused write-set helpers:

- one helper for `session_daily_installs + session_daily upsert + duration-only updates`
- one helper for all five `session_metric_*` cuboid upserts

This directly targets fixed statement overhead while preserving the current Rust-side plan building and invariants.

### 2. More aggressive SQL derivation from inserted sessions

Drive the daily/session writes from `INSERT ... RETURNING` over new `sessions` rows and remove some Rust-built deltas entirely.

This might save more CPU and statements, but it moves more product logic into SQL and makes tail-update and first-seen edge cases riskier.

### 3. Minimal metrics-only fusion

Only fuse the five session metric cuboid upserts and leave the `session_daily*` chain unchanged.

This is the safest cut, but it likely leaves too much fixed overhead in place to matter on representative append.

## Recommended Design

### Worker shape

Keep `IncrementalSessionPlan` and `SessionMetricAccumulator` behavior unchanged.

`apply_incremental_session_plan()` should still:

1. insert `install_first_seen` if present
2. update the tail session if present
3. insert new session rows
4. upsert active-install slices
5. build daily deltas and metric deltas exactly as it does today
6. enqueue active-install bitmap rebuild days
7. upsert `install_session_state`

The change is that steps 5 and 6 become fewer store calls, not different semantics.

### New daily write-set helper

Add a store helper, for example:

- `apply_session_daily_write_set_in_tx(tx, write_set) -> Result<SessionDailyWriteOutcome, StoreError>`

where `write_set` packages:

- `SessionDailyInstallDelta`
- `SessionDailyDelta`
- `SessionDailyDurationDelta`

This helper should execute the current three-step daily write set in one round trip using one SQL statement with staged inputs:

1. upsert `session_daily_installs`
2. aggregate returned active-install deltas by day
3. upsert `session_daily`
4. apply duration-only updates for days that must already exist
5. return counts the worker can still use for invariant checks

Important:

- duration-only updates must remain update-only, not silent inserts
- days with session-count or active-install changes may still be handled through the existing upsert semantics
- the helper must preserve today’s exact arithmetic for `sessions_count`, `active_installs`, and `total_duration_seconds`

### New metric write-set helper

Add a store helper, for example:

- `upsert_session_metric_write_set_in_tx(tx, write_set) -> Result<(), StoreError>`

where `write_set` packages the five delta vectors currently emitted by `SessionMetricAccumulator::into_store_deltas()`.

This helper should execute one SQL statement with staged inputs and one `INSERT ... ON CONFLICT DO UPDATE` CTE per cuboid table:

- `session_metric_buckets_total`
- `session_metric_buckets_dim1`
- `session_metric_buckets_dim2`
- `session_metric_buckets_dim3`
- `session_metric_buckets_dim4`

Why this shape:

- the tables are different enough that one generic table-target abstraction is not helpful
- the expensive part on the hot path is the repeated statement boundary, not Rust-side tuple sorting
- a single multi-CTE statement keeps table-specific SQL explicit while removing four extra round trips

### Explicit non-changes

Do not change:

- session rebuild SQL
- repair queue or repair finalization behavior
- active-install bitmap rebuild ownership
- `SessionMetricAccumulator` contents or ordering rules
- `insert_sessions_in_tx()` / `update_session_tail_in_tx()` semantics
- `insert_install_first_seen_in_tx()` semantics

## File Boundaries

Expected primary changes:

- `crates/fantasma-worker/src/worker.rs`
  - switch the incremental append path to the new fused store helpers
  - keep current invariant checks and metric ordering behavior
- `crates/fantasma-store/src/lib.rs`
  - add the new write-set structs and fused helpers
  - keep lower-level helpers only if still used elsewhere
- `docs/STATUS.md`
  - record the slice start, result, and benchmark outcome

Expected tests:

- `crates/fantasma-store/src/lib.rs`
  - new focused tests for the fused daily helper
  - new focused tests for the fused metric helper
- `crates/fantasma-worker/src/worker.rs`
  - regression proving the incremental append path calls the fused helpers without changing metric output
- `crates/fantasma-worker/tests/pipeline.rs`
  - existing end-to-end pipeline coverage should stay green

## Tests

### Store behavior

Add coverage for:

- fused daily helper preserves active-install delta math across repeated same-day inserts
- fused daily helper still treats duration-only updates as update-only
- fused metric helper produces the same stored rows as the old five-helper sequence for a mixed delta set
- empty write sets stay no-op

### Worker behavior

Add or extend coverage for:

- incremental append still emits byte-for-byte equivalent metric deltas and daily results
- first-seen insert still adds `new_installs` exactly once
- tail-only duration extension still updates the existing day without changing session count

### Verification

Required verification for implementation:

- `cargo fmt --all --check`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo test -p fantasma-core --quiet`
- focused store and worker regressions for the new fused helpers
- `FANTASMA_DOCKER_TEST_KEEP_CACHE=1 ./scripts/docker-test.sh -p fantasma-worker --test pipeline --quiet`

## Benchmark Gate

Benchmark against the current accepted Hotspot 3 append baseline recorded in `docs/STATUS.md`.

Required before/after runs:

- `live-append-small-blobs`
- `live-append-plus-light-repair`

Acceptance bar:

- `live-append-small-blobs` append throughput improves or stays flat
- `live-append-plus-light-repair` append throughput improves or stays flat
- repair-only wins do not justify append regressions
- checkpoint session / derived p95 should stay flat or better

If representative append regresses, reject the slice even if repair numbers improve.

## Docs

Update:

- `docs/STATUS.md` when implementation starts
- `docs/STATUS.md` when the benchmark result is known
- `docs/architecture.md` only if the worker’s session append call graph materially changes in a way worth documenting
