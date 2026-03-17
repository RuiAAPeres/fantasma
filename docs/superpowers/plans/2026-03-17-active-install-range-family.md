# Active Installs Exact-Range Session Metrics Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Keep `active_installs` inside `GET /v1/metrics/sessions` while changing it from an aligned bucket-only metric into an exact-range distinct-install metric with optional intervaling, full existing filters, `group_by` up to 2 dimensions, and bounded worker-derived performance.

**Architecture:** `active_installs` remains a session metric and remains install-scoped. The worker maintains daily active-install bitmap cuboids (`total`, `dim1`, `dim2`) from `session_active_install_slices`, and the API answers `/v1/metrics/sessions?metric=active_installs` by unioning daily bitmaps across the exact requested range. Public week/month/year active-install storage goes away; `count`, `duration_total`, and `new_installs` keep their current session-bucket behavior.

**Tech Stack:** Rust, Axum, SQLx, Postgres, `roaring` crate, Fantasma CLI, OpenAPI/docs, Docker-backed Postgres tests.

---

## File Map

### Existing files to modify

- `crates/fantasma-api/src/http.rs`
  Keep `/v1/metrics/sessions`, replace active-install parsing/validation/read behavior, preserve existing count/duration/new-installs path.
- `crates/fantasma-cli/src/app.rs`
  Keep `fantasma metrics sessions --metric active_installs`, update local validation and request serialization.
- `crates/fantasma-cli/src/cli.rs`
  Help text and examples for exact-range `active_installs`.
- `crates/fantasma-cli/tests/http_flows.rs`
  CLI regression coverage for the changed contract.
- `crates/fantasma-core/src/metrics.rs`
  Shared response/query types if the new point shape should be shared.
- `crates/fantasma-store/src/lib.rs`
  Install-ordinal helpers, bitmap tables/queue helpers, bitmap row fetchers, exact-range read helpers, tests.
- `crates/fantasma-store/Cargo.toml`
  Add `roaring`.
- `crates/fantasma-worker/src/worker.rs`
  Queue and rebuild daily active-install bitmap cuboids; remove legacy week/month/year active-install cuboid maintenance.
- `crates/fantasma-worker/tests/pipeline.rs`
  API and worker end-to-end tests for exact-range active installs.
- `scripts/compose-smoke.sh`
  Update session metrics active-installs checks.
- `scripts/cli-smoke.sh`
  Update CLI smoke for the changed query shape.
- `schemas/openapi/fantasma.yaml`
  Rewrite `active_installs` contract under `/v1/metrics/sessions`.
- `docs/architecture.md`
  Replace aligned-bucket-only active-installs description with exact-range semantics and bitmap-backed implementation notes.
- `docs/deployment.md`
  Operator examples for the changed `sessions` route and CLI usage.
- `docs/STATUS.md`
  Record start/completion and verification.

### New files to create

- `crates/fantasma-store/migrations/0018_active_install_daily_bitmaps.sql`
  Project install ordinals, daily bitmap tables, rebuild queue, indexes.

### Runtime paths to retire

- Runtime use of:
  - `active_install_metric_buckets_total`
  - `active_install_metric_buckets_dim1`
  - `active_install_metric_buckets_dim2`
  - their rebuild queue path
- The aligned-only `granularity=day|week|month|year` contract for public `active_installs`

## Correct Public Contract

### Route

- Keep `GET /v1/metrics/sessions`

### Metric surface

- `count`
- `duration_total`
- `new_installs`
- `active_installs`

### Query contract for `active_installs`

- Required:
  - `metric=active_installs`
  - `start=YYYY-MM-DD`
  - `end=YYYY-MM-DD`
- Optional:
  - `interval=day|week|month|year`
  - repeated `group_by`
  - equality filters on built-ins and explicit first-event session properties

### Semantics

- Counts distinct installs with at least one session whose `session_start` falls inside the exact requested UTC date range.
- `start` and `end` are inclusive UTC dates.
- Without `interval`, return one exact-range value for the whole request.
- With `interval`, partition the exact request range into logical windows and return one value per window.
- For `week|month|year`, the windows are calendar-shaped but clipped to the exact request edges. They are not alignment-gated request parameters.
- Grouped or filtered slices may overlap. Group totals do not need to sum back to the ungrouped total.

### Response shape for `active_installs`

Do not force `active_installs` into the old `bucket` response shape. It needs exact window boundaries.

```json
{
  "metric": "active_installs",
  "start": "2026-03-01",
  "end": "2026-03-17",
  "interval": "week",
  "group_by": ["platform"],
  "series": [
    {
      "dimensions": { "platform": "ios" },
      "points": [
        { "start": "2026-03-01", "end": "2026-03-02", "value": 41 },
        { "start": "2026-03-03", "end": "2026-03-09", "value": 77 },
        { "start": "2026-03-10", "end": "2026-03-16", "value": 63 },
        { "start": "2026-03-17", "end": "2026-03-17", "value": 12 }
      ]
    }
  ]
}
```

### Query contract for the other session metrics

- `count`, `duration_total`, and `new_installs` keep the current bucketed session-metrics behavior.
- Do not redesign the whole sessions family in this slice.
- If the shared handler needs metric-specific parsing branches, that is acceptable.

### Hard limits for `active_installs`

- Distinct referenced dimensions across filters plus `group_by`: max 2.
- Returned point cap for v1: 120.
- Keep existing group-count guardrails.
- Do not add a blanket exact-range span cap in v1. If a second safety bound is
  needed later, make it evidence-driven and interval-aware instead of hardcoding
  a flat day-count limit.

## Storage Design

### Source of truth

- Keep `session_active_install_slices` as the worker-owned per-day install-membership source.
- Public reads for `active_installs` must stop querying it directly.
- Public reads must go through worker-built daily bitmaps so exact-range and intervaled reads share one semantic model.

### New tables

- `project_install_ordinals`
  - `(project_id, install_id) -> ordinal`
  - unique `(project_id, ordinal)`
- `project_install_ordinal_state`
  - `(project_id, next_ordinal)`
- `active_install_bitmap_rebuild_queue`
  - `(project_id, day)` touched-day queue
- `active_install_daily_bitmaps_total`
  - `(project_id, day, bitmap, cardinality, updated_at)`
- `active_install_daily_bitmaps_dim1`
  - `(project_id, day, dim1_key, dim1_value, dim1_value_is_null, bitmap, cardinality, updated_at)`
- `active_install_daily_bitmaps_dim2`
  - `(project_id, day, dim1_key, dim1_value, dim1_value_is_null, dim2_key, dim2_value, dim2_value_is_null, bitmap, cardinality, updated_at)`

### Bitmap encoding

- Use `roaring::RoaringTreemap`
- Serialize to `BYTEA`
- Store `cardinality BIGINT` beside serialized bytes for assertions/debugging

### Invariants

- An install may appear in multiple dim1/dim2 groups on the same day. Preserve that.
- Union across multiple days in one exact-range window must dedupe the same install within the same group.
- No request-time `COUNT(DISTINCT ...)` scans over `sessions` or raw events for public reads.

## Read Design

### Query normalization

- Parse exact `start` and `end` once.
- Build point windows:
  - no interval: one `[start, end]` point
  - `interval=day`: one UTC day per point
  - `interval=week`: ISO-week-shaped windows clipped to the request edges
  - `interval=month`: month-shaped windows clipped to the request edges
  - `interval=year`: year-shaped windows clipped to the request edges

### Table selection

- 0 referenced keys: total table
- 1 referenced key: dim1 table
- 2 referenced keys: dim2 table

### Group handling

- Storage lookup uses canonical sorted key order.
- Response dimensions must preserve requested `group_by` order.
- Filter-only requests union only the full-intersection rows.
- Grouped requests keep one bitmap union per output group per point window.

### Boundedness

- Request path cost is bounded by:
  - `range_days * matching_groups` bitmap rows
  - in-memory bitmap unions only

## Worker Design

### High-level flow

- Append and repair continue maintaining `session_active_install_slices`.
- Any touched day in that source table must enqueue `(project_id, day)` in `active_install_bitmap_rebuild_queue`.
- A worker drain pass rebuilds the exact touched days into the daily bitmap tables.

### Rebuild algorithm for one `(project_id, day)`

1. Load all slice rows for that day.
2. Allocate missing project-local install ordinals.
3. Build one total bitmap.
4. Build one bitmap per dim1 row.
5. Build one bitmap per dim2 row.
6. In one transaction:
   - delete existing bitmap rows for that day
   - insert the rebuilt rows
   - clear the queue row

### Important runtime rule

- Delete or retire week/month/year active-install cuboid rebuilds. Daily bitmaps are the only maintained active-install aggregate.

## Testing Strategy

- TDD throughout.
- Add failing parser/contract tests first.
- Add failing store exact-range bitmap tests before implementing the read path.
- Add failing worker pipeline tests before implementing rebuild flow.
- Keep DB-backed Rust tests on Docker Postgres only.
- Add EXPLAIN-backed store assertions if read-index coverage changes materially.

## Chunk 1: Lock the Correct Sessions Contract

### Task 1: Add failing API and CLI contract tests

**Files:**
- Modify: `crates/fantasma-api/src/http.rs`
- Modify: `crates/fantasma-cli/tests/http_flows.rs`
- Modify: `crates/fantasma-worker/tests/pipeline.rs`

- [ ] **Step 1: Add failing API parser/unit tests**

Cover:
- `/v1/metrics/sessions?metric=active_installs&start=2026-03-01&end=2026-03-17`
- same query with `interval=week`
- returned-point rejection beyond 120 points
- rejection of 3 referenced dimensions
- `count|duration_total|new_installs` unchanged

- [ ] **Step 2: Run targeted API tests and confirm failure**

Run: `cargo test -p fantasma-api active_installs_query -- --nocapture`

Expected: FAIL because the new exact-range contract is not implemented.

- [ ] **Step 3: Add failing CLI request/validation tests**

Cover:
- `fantasma metrics sessions --metric active_installs --start ... --end ...`
- optional `--interval`
- filters and `--group-by`
- local rejection for over-wide ranges if CLI enforces it

- [ ] **Step 4: Run targeted CLI tests and confirm failure**

Run: `cargo test -p fantasma-cli --test http_flows active_installs -- --nocapture`

Expected: FAIL because CLI serialization/validation still follows the old contract.

- [ ] **Step 5: Commit**

```bash
git add crates/fantasma-api/src/http.rs crates/fantasma-cli/tests/http_flows.rs crates/fantasma-worker/tests/pipeline.rs
git commit -m "test: define exact-range active_installs sessions contract"
```

### Task 2: Implement metric-specific sessions parsing and response wiring

**Files:**
- Modify: `crates/fantasma-api/src/http.rs`
- Modify: `crates/fantasma-core/src/metrics.rs`
- Modify: `schemas/openapi/fantasma.yaml`

- [ ] **Step 1: Add active-installs-specific parsing inside the sessions route**

Implement:
- required exact `start`/`end`
- optional `interval`
- metric-specific span/point validation

- [ ] **Step 2: Keep existing parsing/validation for the other session metrics**

- [ ] **Step 3: Add active-installs-specific response encoding**

Use point `start`/`end` for `active_installs`.

- [ ] **Step 4: Update OpenAPI under `/v1/metrics/sessions`**

Document:
- exact-range semantics
- clipped interval windows
- group overlap semantics
- unchanged bucketed semantics for the other session metrics

- [ ] **Step 5: Run targeted tests**

Run:
- `cargo test -p fantasma-api active_installs_query -- --nocapture`
- `cargo test -p fantasma-api parse_session_metric_query -- --nocapture`
- `cargo test -p fantasma-api openapi_documents_scoped_auth_and_event_metrics_contract -- --nocapture`

Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add crates/fantasma-api/src/http.rs crates/fantasma-core/src/metrics.rs schemas/openapi/fantasma.yaml
git commit -m "feat: add exact-range active_installs to sessions metrics"
```

## Chunk 2: Add Daily Bitmap Storage

### Task 3: Add migrations and store helpers

**Files:**
- Create: `crates/fantasma-store/migrations/0018_active_install_daily_bitmaps.sql`
- Modify: `crates/fantasma-store/src/lib.rs`
- Modify: `crates/fantasma-store/Cargo.toml`

- [ ] **Step 1: Write failing migration/store tests**

Cover:
- new tables exist
- new indexes exist
- ordinal allocation helpers work
- queue helpers work

- [ ] **Step 2: Run targeted tests and confirm failure**

Run: `./scripts/docker-test.sh -p fantasma-store active_install_bitmap_ -- --nocapture`

Expected: FAIL because migration/helpers do not exist yet.

- [ ] **Step 3: Add migration**

Create:
- ordinal tables
- bitmap rebuild queue
- total/dim1/dim2 bitmap tables

- [ ] **Step 4: Add store helpers**

Implement:
- missing-ordinal allocation
- enqueue/load/delete queue rows
- replace bitmap rows for one day
- fetch bitmap rows by exact day range and dimension keys

- [ ] **Step 5: Add bitmap serialization helpers**

- [ ] **Step 6: Run targeted store tests**

Run: `./scripts/docker-test.sh -p fantasma-store active_install_bitmap_ -- --nocapture`

Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add crates/fantasma-store/migrations/0018_active_install_daily_bitmaps.sql crates/fantasma-store/src/lib.rs crates/fantasma-store/Cargo.toml
git commit -m "feat: add daily bitmap storage for active_installs"
```

### Task 4: Add failing exact-range bitmap read tests, then implement reads

**Files:**
- Modify: `crates/fantasma-store/src/lib.rs`

- [ ] **Step 1: Add failing store tests**

Cover:
- no-interval exact-range dedupe across days
- `interval=week` clipped edge windows
- grouped overlap semantics
- dim2 filter intersection behavior
- requested `group_by` order in output

- [ ] **Step 2: Run targeted tests and confirm failure**

Run: `./scripts/docker-test.sh -p fantasma-store active_install_range_query_ -- --nocapture`

Expected: FAIL because the read path is not implemented.

- [ ] **Step 3: Implement exact-range bitmap read helpers**

Implement:
- point-window generation
- canonical dimension-key lookup
- row loading from total/dim1/dim2 tables
- in-memory unions and cardinality counts

- [ ] **Step 4: Run targeted tests**

Run: `./scripts/docker-test.sh -p fantasma-store active_install_range_query_ -- --nocapture`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/fantasma-store/src/lib.rs
git commit -m "feat: add exact-range bitmap reads for active_installs"
```

## Chunk 3: Wire the Worker and Remove Legacy Range Cuboids

### Task 5: Add failing worker tests, then rebuild exact touched days

**Files:**
- Modify: `crates/fantasma-worker/src/worker.rs`
- Modify: `crates/fantasma-worker/tests/pipeline.rs`

- [ ] **Step 1: Add failing pipeline tests**

Cover:
- append enqueues touched days
- repair enqueues touched days
- worker rebuilds daily bitmaps
- exact-range active-installs query returns correct totals
- intervaled active-installs query returns clipped windows

- [ ] **Step 2: Run targeted worker tests and confirm failure**

Run: `./scripts/docker-test.sh -p fantasma-worker --test pipeline active_installs_ -- --nocapture`

Expected: FAIL because the worker does not rebuild bitmap tables yet.

- [ ] **Step 3: Implement queueing and rebuild flow**

Implement:
- touched-day enqueue when `session_active_install_slices` change
- bitmap queue drain
- per-day rebuild transaction

- [ ] **Step 4: Remove legacy week/month/year active-install cuboid runtime usage**

- [ ] **Step 5: Run targeted worker tests**

Run: `./scripts/docker-test.sh -p fantasma-worker --test pipeline active_installs_ -- --nocapture`

Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add crates/fantasma-worker/src/worker.rs crates/fantasma-worker/tests/pipeline.rs
git commit -m "feat: rebuild active_installs from daily bitmap queue"
```

## Chunk 4: Dogfood the Sessions Contract Through CLI and Smokes

### Task 6: Update CLI and smoke scripts without changing command family

**Files:**
- Modify: `crates/fantasma-cli/src/app.rs`
- Modify: `crates/fantasma-cli/src/cli.rs`
- Modify: `crates/fantasma-cli/tests/http_flows.rs`
- Modify: `scripts/cli-smoke.sh`
- Modify: `scripts/compose-smoke.sh`

- [ ] **Step 1: Keep `fantasma metrics sessions --metric active_installs`**

Update it to serialize:
- `--start`
- `--end`
- optional `--interval`
- filters
- `--group-by`

- [ ] **Step 2: Remove old aligned-only active-installs validation from CLI**

- [ ] **Step 3: Update smoke scripts**

Replace old examples like:
- `granularity=week&start=...aligned...`

With exact-range examples like:
- `metric=active_installs&start=2026-03-01&end=2026-03-17`
- optional `interval=week`

- [ ] **Step 4: Run CLI and smoke checks**

Run:
- `cargo test -p fantasma-cli --test http_flows -- --nocapture`
- `bash -n scripts/cli-smoke.sh scripts/compose-smoke.sh`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/fantasma-cli/src/app.rs crates/fantasma-cli/src/cli.rs crates/fantasma-cli/tests/http_flows.rs scripts/cli-smoke.sh scripts/compose-smoke.sh
git commit -m "feat: dogfood exact-range active_installs via sessions CLI"
```

## Chunk 5: Docs and Verification

### Task 7: Rewrite docs around the corrected sessions design

**Files:**
- Modify: `schemas/openapi/fantasma.yaml`
- Modify: `docs/architecture.md`
- Modify: `docs/deployment.md`
- Modify: `docs/STATUS.md`

- [ ] **Step 1: Update docs**

Document:
- `active_installs` stays under `/v1/metrics/sessions`
- exact `start`/`end` semantics
- optional `interval`
- clipped week/month/year windows
- bitmap-backed daily storage

- [ ] **Step 2: Record STATUS start/completion and verification**

- [ ] **Step 3: Commit**

```bash
git add schemas/openapi/fantasma.yaml docs/architecture.md docs/deployment.md docs/STATUS.md
git commit -m "docs: rewrite active_installs sessions contract"
```

### Task 8: Run final verification

**Files:**
- No code changes expected unless verification exposes defects

- [ ] **Step 1: Run format and lint**

Run:
- `cargo fmt --all --check`
- `cargo clippy --workspace --all-targets -- -D warnings`

- [ ] **Step 2: Run focused package tests**

Run:
- `cargo test -p fantasma-api -- --nocapture`
- `cargo test -p fantasma-cli --test http_flows -- --nocapture`
- `./scripts/docker-test.sh -p fantasma-store active_install_ -- --nocapture`
- `./scripts/docker-test.sh -p fantasma-worker --test pipeline active_installs_ -- --nocapture`

- [ ] **Step 3: Run smoke checks**

Run:
- `bash -n scripts/cli-smoke.sh scripts/compose-smoke.sh`

- [ ] **Step 4: Run a benchmark-sensitive read proof**

Run at minimum:

```bash
cargo run -p fantasma-bench -- slo --output-dir artifacts/performance/2026-03-17-active-install-range-family --scenario reads-visibility-30d
```

Expected:
- public active-installs reads do not scan `sessions` directly
- representative 30-day exact-range reads stay bounded and low-latency

- [ ] **Step 5: Update `fantasma-bench` if needed**

If the benchmark matrix assumes aligned weekly/monthly/yearly `active_installs`, rewrite it for exact-range semantics in the same slice.

- [ ] **Step 6: Commit verification-driven fixes if needed**

```bash
git add -A
git commit -m "test: verify exact-range active_installs sessions metric"
```

## Execution Rules for the Next Agent

- Do not split `active_installs` out of `/v1/metrics/sessions`.
- Do not redesign `count`, `duration_total`, or `new_installs` in this slice.
- Do not reintroduce request-time `COUNT(DISTINCT ...)` scans for public reads.
- Preserve install-scoped language and semantics.
- Preserve grouped overlap semantics.
- Keep all DB-backed verification on Docker Postgres.
- Treat this as a breaking contract cleanup if necessary; do not waste time on compatibility shims unless explicitly asked.

## Open Decisions to Resolve Only if Implementation Forces Them

- Whether the active-installs response types should share code with existing metric responses or stay metric-local.
- Whether `fantasma-bench` should treat exact-range active-installs reads as visibility-only or hard-gated.

Plan complete and saved to `docs/superpowers/plans/2026-03-17-active-install-range-family.md`. Ready to execute.
