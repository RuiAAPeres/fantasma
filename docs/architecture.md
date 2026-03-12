# Architecture

## Principles

- Everything is an event.
- Events are immutable.
- Aggregation is asynchronous.
- APIs are public by default.
- SDK behavior must be explicit.
- Deployment must stay simple.
- Fantasma is install-scoped, not person-scoped.
- Privacy claims must be reflected in the data model.
- Fantasma prefers simple, explicit, high-performance data paths.
- We avoid steady-state designs that rely on broad recomputation.
- We narrow feature shape when necessary to preserve predictable performance and operational clarity.
- Correctness still matters, but we should choose correctness models that compose with incremental processing.

## High-Level Flow

```text
curl / SDK
  -> POST /v1/events
  -> Rust ingest service
  -> append-only Postgres events_raw table
  -> fantasma-worker
  -> derived Postgres sessions, session_daily, session_daily_installs, event_count_daily_total, event_count_daily_dim1, event_count_daily_dim2, and event_count_daily_dim3 tables
  -> GET /v1/metrics/sessions/*, /v1/metrics/sessions/count/daily, /v1/metrics/sessions/duration/total/daily, /v1/metrics/events/aggregate, and /v1/metrics/events/daily
  -> query API
```

## Services

### Ingest Service

Responsibilities:

- authenticate project-scoped ingest keys
- validate incoming event batches
- batch insert raw events into Postgres
- acknowledge accepted events with `202 Accepted`

Non-responsibilities:

- synchronous aggregation
- hidden identity mutation
- dashboard-specific responses

### Worker

Responsibilities:

- process raw events asynchronously
- derive session records from raw events
- populate aggregate tables for product metrics
- keep processing idempotent
- support version-aware mobile analytics

Current derived metric:

- sessions
- session_daily
- session_daily_installs
- event_count_daily_total
- event_count_daily_dim1
- event_count_daily_dim2
- event_count_daily_dim3

Current worker behavior:

- poll `events_raw` in batches ordered by raw event id
- group events by `project_id` and `install_id`
- load one persisted tail state row per `(project_id, install_id)` from `install_session_state`
- sort each install batch by `(timestamp, id)` and process forward only
- infer sessions from event timestamps with a 30-minute inactivity rule
- keep at most one mutable tail session per install
- extend the current tail session in place when a new event is newer than or equal to the tail end and inside the inactivity window
- insert a new immutable session row and replace tail state when a new event exceeds the inactivity window
- switch an install into a bounded repair path when a batch contains older-than-tail events
- derive replacement sessions from raw events inside the overlapping repair window and rewrite only those derived rows
- rebuild only the exact touched UTC start days for `session_daily` and `session_daily_installs` after a repair
- keep the normal append path incremental: `session_daily` still updates directly from new sessions and tail-session duration deltas
- update `session_daily_installs` incrementally so daily active installs never depend on `COUNT(DISTINCT ...)` rebuilds
- derive event-count cuboids incrementally from raw events into daily totals, single-dimension rollups, two-dimension rollups, and three-dimension rollups
- keep event-metrics rollups keyed only by canonical string dimensions, never by the full property bag
- advance a `worker_offsets` checkpoint only after successful writes

Current aggregate ownership:

- `sessions` stores immutable closed sessions plus one mutable tail session per install
- `install_session_state` is the worker's steady-state decision surface for sessionization
- `session_daily` stores UTC `DATE` buckets for `sessions_count`, `active_installs`, and `total_duration_seconds`
- `session_daily_installs` stores per-day install membership and session counts so internal daily active-install aggregates stay incremental without `COUNT(DISTINCT ...)` rebuilds
- `event_count_daily_total` stores per-project, per-event, per-day totals
- `event_count_daily_dim1`, `event_count_daily_dim2`, and `event_count_daily_dim3` store bounded sparse cuboids keyed by canonical dimension slots

Current public daily metric scope:

- `sessions_count_daily`
- `session_duration_total_daily`
- `event_count_daily`

Current public aggregate metric scope:

- `event_count`

Planned aggregates:

- screen views by day
- retention cohorts
- version adoption
- custom event counts by day

### API Service

Responsibilities:

- expose project-scoped query endpoints
- parse the explicit event-metrics query surface from raw query params
- read only worker-derived aggregates for analytics queries
- provide a stable public API for first-party and third-party clients

## Data Model Direction

Core persisted concepts:

- `projects`
- `api_keys`
- `events_raw`
- `sessions`
- `session_daily`
- `worker_offsets`
- aggregate tables keyed by project, metric window, and dimensions

Event requirements:

- `event`
- `timestamp`
- `install_id`
- `platform`

Optional event fields:

- `app_version`
- `os_version`
- at most 3 explicit string `properties`

Identity model:

- `install_id` is the public analytics unit for the MVP
- Fantasma does not expose person-scoped identifiers in the public event contract
- backend-derived session identifiers are internal implementation details, not public API
- multi-device usage counts as multiple installs by design
- `properties` are for event context and must not contain direct identifiers or sensitive personal data
- built-in event dimensions stay minimal: `platform`, `app_version`, and `os_version`

## Event Metrics Query Model

Event metrics use exactly two endpoint shapes:

- `GET /v1/metrics/events/aggregate`
- `GET /v1/metrics/events/daily`

Query semantics:

- `project_id`, `event`, `start_date`, and `end_date` are required
- built-in equality filters use normal query params: `platform`, `app_version`, `os_version`
- any other non-reserved query key matching `^[a-z][a-z0-9_]{0,62}$` is an equality filter on an explicit string property
- `group_by` may be repeated up to twice
- the total number of distinct referenced dimensions across filters plus `group_by` is capped at 3
- reads come only from the worker-built daily cuboids, so the surface is explicitly eventually consistent

Missing-dimension behavior:

- grouped event metrics synthesize explicit `null` buckets from lower-order cuboids so grouped totals add back up to the filtered total
- the worker does not persist synthetic null rows

## SDK Direction

The first SDK target is iOS.

Required client behavior:

- `configure(serverURL, writeKey)`
- `track(eventName, properties?)`
- `flush()`
- `clear()`

Durability requirement:

- persist each event before attempting upload
- use local SQLite for the queue
- delete only acknowledged rows
- preserve queued events across `clear()`

Current iOS SDK shape:

- expose a single static `Fantasma` facade backed by one shared client
- store `install_id` in local defaults
- serialize each tracked event to JSON and store it as an immutable SQLite row
- auto-populate `platform`, `app_version`, and `os_version`
- upload queued events asynchronously to `POST /v1/events` using the existing batch contract

Current upload triggers:

- every 10 seconds
- when the local queue reaches 50 events
- when `flush()` is called
- when the app enters background

Current identity rules:

- `install_id` is generated on first use and reused until `clear()`
- `clear()` rotates `install_id` and leaves already queued rows untouched
