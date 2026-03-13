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
operator
  -> GET/POST /v1/projects*
  -> fantasma-api
  -> projects + api_keys in Postgres

curl / SDK
  -> POST /v1/events
  -> Rust ingest service
  -> append-only Postgres events_raw table
  -> fantasma-worker
  -> derived Postgres sessions, session_daily, session_daily_installs, event bucket rollups, session bucket rollups, and install first-seen state
  -> GET /v1/metrics/events and /v1/metrics/sessions
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
- worker-derived hourly and daily event metric cuboids
- worker-derived hourly and daily session metric rollups
- install first-seen state for `new_installs`

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
- rebuild only the exact touched hourly and daily session metric buckets after session changes
- preserve grouped session dimensions from the pre-repair session assignment so late events do not rebucket `platform` or `app_version`
- keep the normal append path incremental: `session_daily` still updates directly from new sessions and tail-session duration deltas
- update `session_daily_installs` incrementally so daily active installs never depend on `COUNT(DISTINCT ...)` rebuilds
- derive event-count cuboids incrementally from raw events into bounded hourly and daily totals, single-dimension rollups, two-dimension rollups, and three-dimension rollups
- derive session metric rollups incrementally for `count`, `duration_total`, and `new_installs`
- keep event-metrics rollups keyed only by canonical string dimensions, never by the full property bag
- advance a `worker_offsets` checkpoint only after successful writes

Current aggregate ownership:

- `sessions` stores immutable closed sessions plus one mutable tail session per install
- `install_session_state` is the worker's steady-state decision surface for sessionization
- `session_daily` stores UTC `DATE` buckets used by bounded session repair and incremental session-count / duration maintenance
- `session_daily_installs` stores per-day install membership so internal install bookkeeping stays incremental without `COUNT(DISTINCT ...)` rebuilds
- bounded event metric cuboids store worker-derived hourly and daily series for event counts
- bounded session metric rollups store worker-derived hourly and daily series for `count`, `duration_total`, and `new_installs`
- `install_first_seen` fixes each install to the bucket of its first accepted event and never rebuckets late arrivals

Planned aggregates:

- screen views by day
- retention cohorts
- version adoption
- custom event counts by day

### API Service

Responsibilities:

- expose operator-authenticated project/key management routes
- expose project-scoped query endpoints
- parse the explicit event-metrics query surface from raw query params
- read only worker-derived aggregates for analytics queries
- provide a stable public API for first-party and third-party clients

Auth model:

- one static operator bearer token is reserved for `/v1/projects*`
- project keys are scoped to exactly one project and one key kind
- `ingest` keys authenticate `POST /v1/events`
- `read` keys authenticate `/v1/metrics/*`
- metrics project scope is derived from the authenticated key, not from a public `project_id` parameter

## Data Model Direction

Core persisted concepts:

- `projects`
- `api_keys`
- `events_raw`
- `sessions`
- `session_daily`
- `worker_offsets`
- aggregate tables keyed by project, metric window, and dimensions

Current `api_keys` shape:

- stores one row per project-scoped secret
- keeps only hash plus redacted prefix, never the plaintext key
- tracks a required `kind` of `ingest` or `read`
- supports soft revocation through `revoked_at`

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

- `install_id` is the client-supplied install identity used by ingest, worker processing, and backend sessionization
- Fantasma's public metrics API does not expose install-level filtering or grouping
- Fantasma does not expose person-scoped identifiers in the public event contract
- backend-derived session identifiers are internal implementation details, not public API
- multi-device usage counts as multiple installs by design
- `properties` are for event context and must not contain direct identifiers or sensitive personal data
- built-in event dimensions stay minimal: `platform`, `app_version`, and `os_version`

## Metrics Query Model

Public metrics use exactly two endpoint shapes:

- `GET /v1/metrics/events`
- `GET /v1/metrics/sessions`

Shared query semantics:

- authenticated project scope comes from the caller's `read` key
- `metric`, `granularity`, `start`, and `end` are required
- `granularity=day` uses inclusive UTC `YYYY-MM-DD` buckets
- `granularity=hour` uses inclusive UTC RFC3339 hour-start buckets
- responses are series-only and always include `metric`, `granularity`, `group_by`, and `series`
- ungrouped responses always use `group_by: []` and one series item with `dimensions: {}`

Event metrics:

- only `metric=count` is public
- `event` is required
- built-in equality filters use `platform`, `app_version`, and `os_version`
- any other non-reserved query key matching `^[a-z][a-z0-9_]{0,62}$` is an equality filter on an explicit string property
- `group_by` may be repeated up to twice
- the total number of distinct referenced dimensions across filters plus `group_by` is capped at 3
- grouped event metrics synthesize explicit `null` buckets from lower-order cuboids so grouped totals add back up to the filtered total

Session metrics:

- supported public metrics are `count`, `duration_total`, and `new_installs`
- `group_by` and equality filters are limited to `platform` and `app_version`
- session grouped dimensions are fixed from the session's first event
- `duration_total` is assigned to the bucket where the session starts
- `new_installs` is assigned once from the install's first accepted event and is never retroactively moved by late arrivals

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
- serialize each tracked event to JSON and store it as an immutable SQLite row
- auto-populate `platform`, `app_version`, and `os_version`
- upload queued events asynchronously to `POST /v1/events` using an `ingest` key only

Current upload triggers:

- every 10 seconds
- when the local queue reaches 50 events
- when `flush()` is called
- when the app enters background

Current identity rules:

- the SDK generates one local install identity on first use, reuses it until `clear()`, and leaves already queued rows untouched when that identity rotates
