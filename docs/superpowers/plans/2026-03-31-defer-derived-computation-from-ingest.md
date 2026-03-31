# Append-First Ingest With Fully Deferred Derived Projections

## Summary

- Keep `POST /v1/events` success defined as: auth succeeded, payload validated, and raw rows were durably appended to `events_raw`.
- Define raw append itself as the durable schedule for downstream work. Ingest does not write projection markers.
- Keep the public metrics route surface and JSON response shapes unchanged.
- Fully defer recomputable customer-facing projections in this slice: event metrics, session metrics, `live_installs`, `active_installs`, `session_daily`, `session_active_install_slices`, and active-install bitmaps.
- Keep raw-backed discovery helpers immediately append-visible: `GET /v1/metrics/events/catalog` and `GET /v1/metrics/events/top`.
- Do not add request-path recomputation from raw tables or new freshness metadata.

## Key Changes

### Contract and docs

- Update `docs/architecture.md`, `docs/STATUS.md`, and `schemas/openapi/fantasma.yaml` so they say:
  - accepted ingest means durable raw append
  - raw append is the durable schedule for deferred downstream computation
  - all derived metrics are eventually consistent
  - raw-backed discovery helpers remain immediate append-visible
- Remove wording that implies append-adjacent freshness for any derived metric.

### Deferred scheduling and queue design

- Add worker-owned coalescing queues:
  - `event_metric_deferred_rebuild_queue(project_id, granularity, bucket_start, event_name)`
  - `session_projection_deferred_rebuild_queue(project_id, granularity, bucket_start)`
  - `install_activity_deferred_rebuild_queue(project_id, install_id)`
- Use PK/unique coalescing on those exact keys.
- Do not enqueue these queues in ingest.
- Enqueue them only from worker hot lanes or rebuild paths after foundational state is determined.

### Atomicity rules

- Event hot lane:
  - enqueue touched event rebuild scope and save raw event-metrics offset progress in the same transaction
  - never advance the raw offset before deferred rebuild scope is durably queued
- Session hot lane:
  - write foundational session state and enqueue deferred session/install projection work in the same session-lane transaction
  - only advance the raw `"sessions"` offset after that transaction commits successfully
  - never allow foundational session state to commit without the matching deferred projection scope being durably queued

### Hot paths

- Ingest path:
  - keep it at auth, parse/normalize/validate, and raw append only
  - do not write derived tables
  - do not write deferred projection queues

- Event worker hot lane:
  - stop upserting `event_metric_buckets_total` and all grouped event bucket tables on the append-adjacent path
  - after processing a raw batch, enqueue touched event rebuild scope inside the same transaction that advances the event offset
  - do not update `live_install_state` here

- Session worker hot lane:
  - keep only foundational state writes needed for eventual correctness:
    - `sessions`
    - `install_first_seen`
    - `install_session_state`
  - stop append-adjacent writes to:
    - `session_daily`
    - `session_metric_buckets_*`
    - `live_install_state`
    - `session_active_install_slices`
    - active-install bitmap tables
  - enqueue touched session projection scope and touched installs inside the same foundational-state transaction

### Deferred drains

- Use the existing worker process and existing lane structure. Do not add new top-level services in this slice.
- Drain projection queues outside the raw-offset transaction, after the relevant hot lane catches up.
- Event deferred drain rebuilds all event metric buckets from `events_raw`.
- Session projection deferred drain rebuilds `session_daily` and `session_metric_buckets_*` from foundational session state.
- Install activity deferred drain rebuilds:
  - `live_install_state` from raw events for touched installs
  - `session_active_install_slices` from foundational session state for touched installs
  - active-install bitmap tables from the rebuilt install activity state
- Derived reads keep serving the last committed projection rows until drains catch up.

### Rare correctness-heavy paths

- Keep repair, range-delete, and purge behavior conservative and full-scope in this slice.
- Those paths must still leave all projection tiers correct before project reactivation.
- Add cleanup for the new deferred queues to range-delete and purge flows.

### Schema and index work

- Add forward migrations for the three new queues.
- Add a forward migration that drops `idx_events_device`.
- Update migration-shape and helper tests to expect the new queues and the removal of that index.

## Public Interfaces

- No route, query-param, status-code, or JSON-shape change for:
  - `POST /v1/events`
  - `GET /v1/metrics/events`
  - `GET /v1/metrics/sessions`
  - `GET /v1/metrics/live_installs`
  - `GET /v1/metrics/events/catalog`
  - `GET /v1/metrics/events/top`
- No SDK API change.
- No event-schema change.

## Test Plan

- Add or update regressions proving:
  - accepted ingest requires only durable raw append
  - the event hot lane enqueues deferred rebuild scope and does not write read-facing projections
  - event offset progress and event deferred scope are committed atomically
  - the session hot lane writes only foundational state and enqueues deferred work
  - foundational session state and deferred session/install scope are committed atomically
  - derived reads can remain stale before deferred drains run
  - the same derived reads become correct after deferred drains complete
  - raw-backed discovery helpers remain immediate append-visible after ingest
  - repair, range-delete, and purge still leave all tiers correct
- Replace eager-fanout worker expectations with deferred-queue expectations.
- Keep parser and response-shape tests green.
- Required signoff commands:
  - `cargo fmt --all --check`
  - `cargo clippy --workspace --all-targets -- -D warnings`
  - `./scripts/verify-changed-surface.sh run worker-contract`
  - `./scripts/verify-changed-surface.sh run api-contract`
  - `./scripts/docker-test.sh -p fantasma-worker --test pipeline --quiet`
- Performance proof:
  - rerun the local ingest benchmark first
  - rerun the exact March 31 remote direct-vs-gateway harness with `200 requests`, `concurrency 10`, and `events-per-request` of `1` and `10`
  - do not substitute another benchmark
  - if the March 31 harness is unavailable, record the comparison as blocked

## Assumptions and defaults

- Raw append is the durable schedule for downstream work.
- Projection-specific queues are second-stage, worker-owned scheduling, not ingest-owned scheduling.
- `install_activity_deferred_rebuild_queue` is keyed by `(project_id, install_id)`.
- Any recomputable customer-facing projection is allowed to lag in this slice.
- No request-path fallback recomputation from raw tables.
- No new public API error codes, response fields, or freshness metadata in this slice.
- `docs/STATUS.md` is updated at both start and finish.
- This plan replaces the earlier March 31 draft in full.
