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
  -> GET/POST/PATCH /v1/projects*, GET /v1/usage/events, and GET /v1/metrics/*
  -> fantasma-api
  -> projects + api_keys in Postgres

curl / SDK
  -> POST /v1/events
  -> Rust ingest service
  -> append-only Postgres events_raw table
  -> fantasma-worker
  -> derived Postgres sessions, session_daily, session_daily_installs, session_active_install_slices, active-install daily bitmaps, live_install_state, event bucket rollups, session bucket rollups, and install first-seen state
  -> GET /v1/metrics/events, /v1/metrics/sessions, and /v1/metrics/live_installs
  -> GET /v1/metrics/events/catalog and /v1/metrics/events/top
  -> query API
```

## Services

### Ingest Service

Responsibilities:

- authenticate project-scoped ingest keys
- validate incoming event batches up to 200 events per request
- batch insert raw events into Postgres
- acknowledge accepted events with `202 Accepted` once raw rows are durably appended

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
- session_active_install_slices
- worker-derived active-install daily bitmaps for exact-range reads
- worker-derived hourly and daily event metric cuboids
- worker-derived hourly and daily session metric rollups
- install first-seen state for `new_installs`

Current worker behavior:

- run `session_apply`, `session_repair`, `event_metrics`, and `project_deletions` lanes concurrently inside the same worker process
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
- keep append-path success scoped to foundational state only: `sessions`, `install_first_seen`, `install_session_state`, and worker-owned deferred queue rows are the only durable session-lane writes required before the coordinator advances
- enqueue deferred scope atomically with the worker-owned state it depends on: the event lane saves offset progress in the same transaction as touched event rebuild scope, and the session lane queues touched session/install projection scope in the same transaction as foundational session state
- treat all customer-facing derived tables as eventually consistent: `event_metric_buckets_*`, `session_metric_buckets_*`, `session_daily`, `session_active_install_slices`, `active_install_daily_bitmaps_*`, and `live_install_state` can lag until deferred drains catch up
- keep bucketed event and session projections bounded at D2; there is no public or internal D3/D4 cuboid surface in the current product
- keep deferred drains outside the raw-offset transactions and let reads serve the last committed derived rows while those drains catch up
- keep repair/backfill conservative and exact: touched-day and touched-bucket rebuilds still recompute from source-of-truth tables instead of trying to preserve incremental deltas across rare correctness-heavy paths
- let `session_repair` own the install-scoped repair commit: rewritten sessions, `install_session_state`, exact touched-day rebuilds, queued touched hour/day bucket finalization, and repair-frontier completion all commit together
- queue shared project hour/day session bucket rebuilds through `session_rebuild_queue` and drain that queue after repair claims finish, even if a later install in the same batch fails, so overlapping installs rebuild each bucket on one serialized path without stranding already-committed repairs behind a later error
- preserve fixed built-in grouped dimensions from the pre-repair session assignment so late events do not rebucket `platform`, `device`, or version dimensions
- keep the request path and append-adjacent worker hot lanes free of direct customer-facing projection writes
- rebuild event metrics from `events_raw`, session metrics from foundational session state, and install activity views from raw events plus foundational session state
- keep event-metrics rollups keyed only by canonical string dimensions, never by the full property bag
- advance a `worker_offsets` checkpoint only after successful writes
- treat project deletion as an explicit operator-owned job: the API flips project lifecycle state and records an independent `project_deletions` row, then the `project_deletions` lane waits for the existing worker queues to drain before it either rebuilds the narrowed project state for a range delete or explicitly purges every project-owned table before deleting the `projects` row

Current aggregate ownership:

- `sessions` stores immutable closed sessions plus one mutable tail session per install
- `install_session_state` is the worker's durable per-install sessionization and replay-progress state
- `session_repair_jobs` stores install-scoped durable repair frontier state for the queue-driven repair lane
- `session_daily` stores UTC `DATE` buckets used by bounded session repair and incremental session-count / duration maintenance
- `session_daily_installs` stores per-day install membership so internal install bookkeeping stays incremental without `COUNT(DISTINCT ...)` rebuilds
- `session_active_install_slices` stores the worker-owned per-day, per-install, fixed-dimension membership rows that feed public exact-range `active_installs`
- `active_install_daily_bitmaps_total` / `dim1` / `dim2` store one canonical worker-maintained daily bitmap family for bounded `active_installs` reads across ungrouped, filtered, and grouped queries
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
- expose operator-authenticated deletion-job management routes
- expose operator-authenticated usage reads
- expose project-scoped query endpoints
- parse the explicit event-metrics query surface from raw query params
- read worker-derived aggregates for bucketed analytics queries
- read raw accepted events for event discovery helpers such as catalog and top-events
- read raw accepted events for operator usage totals across projects
- provide a stable public API for first-party and third-party clients

Current operator access pattern:

- the Fantasma CLI is the primary manual operator surface for remote instances
- operators authenticate the CLI with the install-time bearer token
- the CLI stores named instance profiles plus one local `read` key per project
- shell helpers remain useful for smoke checks and automation, not as the main human workflow

Auth model:

- one static operator bearer token is reserved for `/v1/projects*` and `/v1/usage/*`
- project keys are scoped to exactly one project and one key kind
- `ingest` keys authenticate `POST /v1/events`
- `read` keys authenticate `/v1/metrics/*`, including the event discovery helpers
- metrics project scope is derived from the authenticated key, not from a public `project_id` parameter
- the CLI does not add user accounts or alternate auth paths; it wraps the same operator and project-key model directly

## Data Model Direction

Core persisted concepts:

- `projects`
- `project_deletions`
- `project_processing_leases`
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

Current `projects` lifecycle shape:

- `projects.state` is explicit management/read visibility, not an implicit side effect
- allowed lifecycle states are `active`, `range_deleting`, and `pending_deletion`
- `range_deleting` pauses ingest, read-key metrics reads, and project mutations until the delete worker commits the rebuilt state and returns the project to `active`
- `pending_deletion` is fail-closed: the project row stays visible to operator reads until the purge worker succeeds, but ingest, read-key routes, and management mutations are latched off
- `project_deletions` keeps historical job rows independent from `projects`, so deletion history survives after full purge removes the project row

Event requirements:

- `event`
- `timestamp`
- `install_id`
- `platform`

Optional event fields:

- `device`
- `app_version`
- `os_version`
- `locale`

Identity model:

- `install_id` is the client-supplied install identity used by ingest, worker processing, and backend sessionization
- Fantasma's public metrics API does not expose install-level filtering or grouping
- Fantasma does not expose person-scoped identifiers in the public event contract
- backend-derived session identifiers are internal implementation details, not public API
- multi-device usage counts as multiple installs by design
- `platform` is the coarse OS family (`ios`, `android`, `macos`)
- `device` is the coarse form factor (`phone`, `tablet`, `desktop`, `unknown`)
- built-in event dimensions stay minimal: `platform`, `device`, `app_version`, `os_version`, and `locale`

## Metrics Query Model

Operator usage reads currently add one narrow raw-event route:

- `GET /v1/usage/events`

Shared usage semantics:

- operator scope comes from the bearer token, not from a public workspace or `project_id` parameter
- `start` and `end` are required inclusive UTC `YYYY-MM-DD` dates
- usage counts accepted raw events by `events_raw.received_at`, not by client event `timestamp`
- responses return one operator-wide total plus per-project rows for projects with non-zero usage in the requested window

Public bucketed metrics use exactly two endpoint shapes:

- `GET /v1/metrics/events`
- `GET /v1/metrics/sessions`

Public current-state metrics currently add one narrow route:

- `GET /v1/metrics/live_installs`

Public event discovery extends that read surface with:

- `GET /v1/metrics/events/catalog`
- `GET /v1/metrics/events/top`
- both discovery routes accept inclusive UTC `start` / `end`; `catalog` and `top` also use the shared discovery `limit` contract with default `10` and max `50`
- read-key metrics routes, including discovery, return `409` while a project is `range_deleting` or `pending_deletion`

Shared query semantics for the bucketed families:

- authenticated project scope comes from the caller's `read` key
- `metric`, `granularity`, `start`, and `end` are required
- `granularity=day` uses inclusive UTC `YYYY-MM-DD` buckets
- `granularity=hour` uses inclusive UTC RFC3339 hour-start buckets
- responses are series-only and always include `metric`, `granularity`, and `series`
- unfiltered responses use one series item with `dimensions: {}`

Event metrics:

- supported public metrics are `count`
- `event` is required
- built-in equality filters use `platform`, `device`, `app_version`, `os_version`, and `locale`
- only built-in filters and built-in `group_by` keys are accepted
- `group_by` is supported on `platform`, `device`, `app_version`, `os_version`, and `locale`
- total referenced dimensions across filters plus `group_by` are capped at 2
- duplicate `group_by` keys and filter/group overlap are rejected

Session metrics:

- supported public metrics are `count`, `duration_total`, `new_installs`, and `active_installs`
- built-in equality filters use `platform`, `device`, `app_version`, `os_version`, and `locale`
- only built-in filters and built-in `group_by` keys are accepted
- `count`, `duration_total`, and `new_installs` allow built-in `group_by` through D2
- `active_installs` allows built-in `group_by` through D2 only
- duplicate `group_by` keys and filter/group overlap are rejected
- `duration_total` is assigned to the bucket where the session starts
- `new_installs` is assigned once from the install's first accepted event and is never retroactively moved by late arrivals
- `count`, `duration_total`, and `new_installs` stay on the bucketed `granularity` contract and still require `granularity`
- `active_installs` stays under `GET /v1/metrics/sessions` but is not a bucketed metric in the same contract shape
- `active_installs` requires exact inclusive UTC `start` and `end`, rejects `granularity`, and optionally accepts `interval=day|week|month|year`
- `active_installs` counts unique installs with at least one session whose `session_start` falls inside the exact requested UTC range or point window
- `active_installs interval=week|month|year` uses calendar-shaped UTC windows clipped to the request edges; requests do not need to be aligned to calendar boundaries
- public `active_installs` reads union worker-maintained daily bitmaps across the requested days; there is no separate public week/month/year cuboid family
- filtered and grouped `active_installs` reads use the same built-in-only vocabulary, but the public contract stops at D2 so the bitmap path stays bounded

Current-state metrics:

- `live_installs` returns one project-wide value instead of a bucketed series
- `live_installs` accepts no public query parameters
- `live_installs` counts installs whose latest worker-processed raw-event `received_at` is within the last 120 seconds
- `live_installs` reads from worker-owned `live_install_state`, keyed by `(project_id, install_id)`
- `live_install_state` is rebuilt by deferred worker drains rather than by the append-adjacent event path
- the API returns `as_of` and `value` from one SQL statement so the cutoff clock and the count share the same snapshot

## SDK Direction

Fantasma ships first-party mobile SDKs with parity at the event-contract and
runtime-behavior level, not necessarily the exact same public API on every
platform.

Required client behavior:

- configure one destination explicitly
- `track(eventName)`
- `flush()`
- `clear()`

Durability requirement:

- persist each event before attempting upload
- use local SQLite for the queue
- delete only acknowledged rows
- preserve queued events across `clear()`

Current first-party SDK shapes:

- iOS exposes a static `Fantasma` facade backed by one shared client
- Android exposes an instance-based `FantasmaClient(context, FantasmaConfig(...))`
- Flutter exposes an instance-based `FantasmaClient(FantasmaConfig(..., storageNamespace: ...))`
- React Native exposes an explicit `FantasmaClient({ serverUrl, writeKey })`
  JS API with one live client per process; the JS layer validates config and
  reserves that slot eagerly, performs native configure/acquire on first use,
  auto-closes the JS client if first native acquire fails, and then delegates
  durability and upload behavior to the native iOS/Android bridge layer
- this API divergence is intentional and platform-native; parity is at the wire
  contract and runtime behavior, not method-for-method API duplication
- Android still preserves one active destination at a time per process: creating
  a client for a different normalized destination supersedes the previous
  destination after the current upload boundary rather than supporting multiple
  live destinations concurrently, and the superseded runtime retires itself even
  if the old handle is dropped without an explicit `close()`
- Flutter keeps each client isolated by its explicit `storageNamespace`, and
  the current runtime supports only one live client per namespace inside a
  process so queue state, blocked-destination state, and install identity stay
  client-local instead of being implicitly shared
- React Native intentionally follows the existing native iOS/Android storage and
  identity models rather than introducing Flutter-style namespaces; it preserves
  destination switching through a thin native bridge handoff that closes the old
  JS client, keeps the old native destination alive only as long as needed for
  a safe handoff, and then lets future uploads move to the new destination

Shared SDK behavior:

- serialize each tracked event to JSON and store it as an immutable SQLite row
- auto-populate `platform`, `device`, `app_version`, `os_version`, and `locale`
- Apple SDKs emit `platform=macos`, `device=desktop` for native macOS, Mac Catalyst, and iOS-on-Mac desktop-class runs
- Android emits `platform=android` with `device=phone` or `device=tablet`, and falls back to `device=unknown` when the runtime cannot classify device form factor
- upload queued events asynchronously to `POST /v1/events` using an `ingest` key only
- treat malformed `202 Accepted` envelopes as invalid responses and keep queued rows for later operator-visible handling
- snapshot `locale` at enqueue time rather than re-resolving it during upload
- block uploads for destinations that return `401`, `422`, or `409 project_pending_deletion` until the client switches to a different normalized destination or storage namespace
- SDKs that support destination replacement should finish the current upload boundary for the old destination, discard still-queued rows from that old destination even across relaunches, and only then let future uploads target the new destination
- React Native preserves that replacement contract even though its JS surface
  allows only one live client per process at a time

Current upload triggers:

- every 30 seconds
- when the local queue reaches 100 events
- when `flush()` is called
- when the app enters background

Trigger semantics:

- `flush()` is the explicit path that can surface upload failures to the caller
- threshold and lifecycle triggers are best-effort background flush attempts and
  must not make `track()` fail after the event has been durably queued

Current identity rules:

- the SDK generates one local install identity on first use, reuses it until `clear()`, and leaves already queued rows untouched when that identity rotates
- `clear()` remains identity-only; queue deletion is reserved for destination replacement rather than normal identity rotation
