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
  -> fantasma CLI
  -> GET/POST/PATCH /v1/projects* and GET /v1/metrics/*
  -> fantasma-api
  -> projects + api_keys in Postgres

curl / SDK
  -> POST /v1/events
  -> Rust ingest service
  -> append-only Postgres events_raw table
  -> fantasma-worker
  -> derived Postgres sessions, session_daily, session_daily_installs, event bucket rollups, session bucket rollups, and install first-seen state
  -> GET /v1/metrics/events and /v1/metrics/sessions
  -> GET /v1/metrics/events/catalog and /v1/metrics/events/top
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

- run `session_apply`, `session_repair`, and `event_metrics` lanes concurrently inside the same worker process
- keep a separate `worker_offsets` checkpoint for the raw-offset lanes and hold the claimed offset-row lock for the full batch window until the final offset save commits
- repoll immediately after progress and sleep only after an idle tick, so busy lanes stay work-conserving
- poll `events_raw` in batches ordered by raw event id
- group `session_apply` batches by `project_id` and `install_id`
- load one persisted state row per `(project_id, install_id)` from `install_session_state` plus the referenced tail session row when append work needs it
- sort each install batch by `(timestamp, id)` and process forward only
- infer sessions from event timestamps with a 30-minute inactivity rule
- keep at most one mutable persisted tail session per install
- plan append-only batches in memory first, then apply only the net outcome for the install batch
- update the preexisting tail session at most once when append events extend it inside the inactivity window
- insert new immutable session rows for later derived sessions and replace tail state once per install batch
- keep `last_processed_event_id` in `install_session_state` so per-install replay becomes a no-op even if the lane offset is rewound after some install work already committed
- treat `install_session_state` as durable worker state and never delete or reset it during ordinary append, repair, or session rewrites
- treat `install_session_state.last_processed_event_id` as the only authoritative applied replay frontier; pending repair state is routing state only
- widen an install-scoped durable repair frontier when `session_apply` sees older-than-tail work or any newer work for an install that already has repair pending
- let `session_apply` persist incremental append results and repair-frontier updates before it advances the raw `"sessions"` offset
- keep `session_repair` queue-driven and independent from the raw `"sessions"` offset
- claim `session_repair_jobs` in a short transaction and release those row locks before the full replay transaction starts so append work never waits on long repair commits while holding the raw offset lock
- if replay or commit fails after a short claim commit, release that install's repair claim back to the queue in a fresh transaction so later retries can still reclaim the frontier
- track each spawned repair worker's claimed job until it resolves so the coordinator can also release that claim if the worker panics or is externally cancelled after the short claim commit, and retain that task context until claim release actually succeeds so transient cleanup failures can retry without discarding the last handle to the claimed row
- let `session_repair` claim install-scoped repair work safely, fetch raw events by `(project_id, install_id, raw_event_id frontier)`, and no-op stale claims when `last_processed_event_id` already covers the claimed frontier
- if one repair in a batch fails, let any already-claimed peer repairs finish their normal success or cleanup paths before surfacing the first error so those peer claims do not get stranded by coordinator-side cancellation
- derive replacement sessions from raw events inside the overlapping repair window and rewrite only those derived rows
- leave append-derived state final at child-transaction commit time: `sessions`, `install_first_seen`, `install_session_state`, `session_daily_installs`, `session_daily`, and the session metric bucket tables are already correct before the coordinator advances offsets
- persist append outcomes with set-based helpers wherever practical so hot-path writes stay batched instead of reintroducing per-event SQL churn
- update `install_first_seen` on append with one `INSERT ... ON CONFLICT DO NOTHING` attempt at most and derive `new_installs` from the insert result rather than a read-before-write precheck
- update `session_daily_installs` once per touched `(project_id, install_id, day)` on append; `active_installs` contributes at most `0/1` per install-day even when `session_count` for that install-day is greater than `1`
- update `session_daily` from net per-day append deltas only; append-only cross-midnight sessions still keep `duration_total` on the session-start bucket while daily counters remain correct for each affected UTC day
- rebuild only the exact touched UTC days for `session_daily` and `session_daily_installs` after a repair
- rebuild only the exact touched hourly and daily session metric buckets after repair/backfill session changes
- keep append and repair intentionally different maintenance paths in this slice: append commits direct deltas, repair/backfill recomputes exact touched windows from source tables
- let `session_repair` own the install-scoped repair commit: rewritten sessions, `install_session_state`, exact touched-day rebuilds, queued touched hour/day bucket finalization, and repair-frontier completion all commit together
- queue shared project hour/day session bucket rebuilds through `session_rebuild_queue` and drain that queue after repair claims finish, even if a later install in the same batch fails, so overlapping installs rebuild each bucket on one serialized path without stranding already-committed repairs behind a later error
- preserve grouped session dimensions from the pre-repair session assignment so late events do not rebucket `platform` or `app_version`
- keep the normal append path incremental and delta-based: direct session metric bucket deltas replace append-path delete-and-rescan maintenance
- derive event-count cuboids incrementally from raw events into bounded hourly and daily totals, single-dimension rollups, two-dimension rollups, and three-dimension rollups
- derive session metric rollups incrementally for `count`, `duration_total`, and `new_installs`
- keep event-metrics rollups keyed only by canonical string dimensions, never by the full property bag
- advance a `worker_offsets` checkpoint only after successful writes

Current aggregate ownership:

- `sessions` stores immutable closed sessions plus one mutable tail session per install
- `install_session_state` is the worker's durable per-install sessionization and replay-progress state
- `session_repair_jobs` stores install-scoped durable repair frontier state for the queue-driven repair lane
- `session_daily` stores UTC `DATE` buckets used by bounded session repair and incremental session-count / duration maintenance
- `session_daily_installs` stores per-day install membership so internal install bookkeeping stays incremental without `COUNT(DISTINCT ...)` rebuilds
- bounded event metric cuboids store worker-derived hourly and daily series for event counts
- bounded session metric rollups store worker-derived hourly and daily series for `count`, `duration_total`, and `new_installs`
- `install_first_seen` fixes each install to the bucket of its first accepted event and never rebuckets late arrivals

Current freshness/read performance proofing:

- `fantasma-bench slo` measures event-derived readiness and session-derived readiness separately
- `event_metrics_ready_ms` and `session_metrics_ready_ms` are published independently because the worker now runs those families in different internal lanes
- `derived_metrics_ready_ms` is the max of the two family readiness values and represents the public derived-surface freshness ceiling
- grouped public reads are benchmarked only after readiness is reached so read latency is measured independently from ingest freshness

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
- read worker-derived aggregates for bucketed analytics queries
- read raw accepted events for event discovery helpers such as catalog and top-events
- provide a stable public API for first-party and third-party clients

Current operator access pattern:

- the Fantasma CLI is the primary manual operator surface for remote instances
- operators authenticate the CLI with the install-time bearer token
- the CLI stores named instance profiles plus one local `read` key per project
- shell helpers remain useful for smoke checks and automation, not as the main human workflow

Auth model:

- one static operator bearer token is reserved for `/v1/projects*`
- project keys are scoped to exactly one project and one key kind
- `ingest` keys authenticate `POST /v1/events`
- `read` keys authenticate `/v1/metrics/*`, including the event discovery helpers
- metrics project scope is derived from the authenticated key, not from a public `project_id` parameter
- the CLI does not add user accounts or alternate auth paths; it wraps the same operator and project-key model directly

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
- at most 4 explicit string `properties`

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

Public event discovery extends that read surface with:

- `GET /v1/metrics/events/catalog`
- `GET /v1/metrics/events/top`

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
- the total number of distinct referenced dimensions across filters plus `group_by` is capped at 4
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
- reject invalid event property maps before they enter the local queue: at most 4 keys, `^[a-z][a-z0-9_]{0,62}$`, and no reserved query or built-in keys such as `metric`, `granularity`, `start`, `end`, `platform`, `app_version`, or `os_version`
- treat malformed `202 Accepted` envelopes as invalid responses and keep queued rows for later operator-visible handling
- when `configure(serverURL, writeKey)` changes destination, finish the current upload boundary, discard still-queued rows from that old destination even across relaunches, and only then let future uploads target the new destination

Current upload triggers:

- every 10 seconds
- when the local queue reaches 50 events
- when `flush()` is called
- when the app enters background

Current identity rules:

- the SDK generates one local install identity on first use, reuses it until `clear()`, and leaves already queued rows untouched when that identity rotates
- `clear()` remains identity-only; queue deletion is reserved for destination-drift reconfiguration rather than normal identity rotation
