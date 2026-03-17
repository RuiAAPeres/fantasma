# Active Installs Exact-Range Sessions Design

## Goal

Keep `active_installs` inside `GET /v1/metrics/sessions` and change its public
semantics from aligned bucket reads to truthful exact-range distinct-install
reads with optional intervaling.

The dashboard needs one semantic model:

- headline KPI: exact selected range
- charts: the same metric partitioned into subwindows of that exact range

The backend should support both without making clients choose between
incompatible APIs or fake totals.

## Product Direction

Fantasma should keep session metrics together.

In this slice:

- `active_installs` remains a session metric
- `count`, `duration_total`, and `new_installs` remain on the same route
- public `active_installs` stops pretending aligned calendar buckets are the
  only useful product shape
- the implementation remains worker-derived and bounded

This is a product correction, not a cosmetic API tweak.

## Decisions

### Keep

- `GET /v1/metrics/sessions`
- `metric=active_installs`
- existing session filter vocabulary
- existing `group_by` vocabulary and D2 limit
- install-scoped semantics

### Change

- `active_installs` no longer uses the aligned `granularity` contract
- `active_installs` now accepts exact `start` and `end`
- `active_installs` may optionally accept `interval=day|week|month|year`
- week/month/year windows are clipped to the requested exact range instead of
  forcing aligned request boundaries
- the response shape for `active_installs` must expose point `start` and `end`
  rather than a single `bucket`

### Cut

- request-time distinct scans over `sessions` for public reads
- worker-maintained week/month/year active-install cuboids
- any separate public active-installs route

## Public API Contract

### Route Surface

The route surface stays:

- `GET /v1/metrics/events`
- `GET /v1/metrics/events/catalog`
- `GET /v1/metrics/events/top`
- `GET /v1/metrics/sessions`

No new top-level metrics route is added for this slice.

### Sessions Metrics

Supported public session metrics remain:

- `count`
- `duration_total`
- `new_installs`
- `active_installs`

### `active_installs` Query Contract

Required parameters:

- `metric=active_installs`
- `start=YYYY-MM-DD`
- `end=YYYY-MM-DD`

Optional parameters:

- `interval=day|week|month|year`
- repeated `group_by`
- built-in filters: `platform`, `app_version`, `os_version`
- explicit first-event session-property filters

Dimension rules:

- total referenced dimensions across filters plus `group_by` capped at 2
- grouped or filtered active-install slices may overlap

Range rules:

- `start` and `end` are inclusive UTC dates
- exact range span is capped in v1 to preserve predictable reads
- returned point count is capped in v1 to preserve predictable reads

### `active_installs` Semantics

`active_installs` is the number of distinct installs with at least one session
whose `session_start` falls inside the exact requested UTC range.

Without `interval`:

- return one exact-range value

With `interval`:

- partition the exact requested range into subwindows
- `day` uses UTC days
- `week` uses ISO-week-shaped windows clipped to the request edges
- `month` uses calendar-month-shaped windows clipped to the request edges
- `year` uses calendar-year-shaped windows clipped to the request edges

Truthfulness rules:

- no point value may be derived by summing lower-granularity distinct counts
- the exact-range headline value must be deduped across the whole requested
  range
- intervaled values must be deduped within each returned point window

### Response Shape

`active_installs` should not reuse the old bucket-only metrics shape.

Expected shape:

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

The other session metrics may keep their current bucketed response shape in
this slice.

## Architecture Constraints

### Worker-Derived Only

Public `active_installs` reads must remain worker-derived.

Do not implement exact-range `active_installs` via:

- request-time distinct scans over raw events
- request-time distinct scans over `sessions`
- ad hoc SQL `COUNT(DISTINCT install_id)` over large ranges

### One Active-Install Storage Model

Use one maintained active-install aggregate grain only:

- daily install-membership bitmaps

Do not keep both:

- exact-range bitmap unions
- separate public week/month/year active-install cuboids

That would reintroduce two semantic models for the same metric.

### Postgres-Only

The design must stay deployable on plain Postgres.

No Redis, Kafka, ClickHouse, or other new runtime dependencies.

## Storage Design

### Source of Truth

Keep `session_active_install_slices` as the worker-owned daily source of
install membership and session-scoped dimensions.

The public API should stop reading that table directly.

### Derived Storage

Add worker-maintained daily bitmap tables for:

- total membership
- dim1 membership
- dim2 membership

Add project-local install ordinals so one install maps to one stable bitmap
member id per project.

Add a touched-day rebuild queue so append and repair can rebuild only the exact
days that changed.

### Encoding

Use serialized roaring bitmaps in Postgres `BYTEA`.

Store cardinality beside each bitmap for assertions, debugging, and cheap
single-row visibility.

## Read Design

### Query Normalization

Normalize each `active_installs` request into:

- exact range `[start, end]`
- zero, one, or two referenced dimension keys
- one logical point window or many intervaled point windows

### Row Selection

Read from:

- total bitmap table for 0 referenced keys
- dim1 bitmap table for 1 referenced key
- dim2 bitmap table for 2 referenced keys

### Union Strategy

For each returned point:

- load matching daily bitmap rows across the exact day range for that point
- union them in memory
- count cardinality of the merged bitmap

For grouped reads:

- maintain one in-memory union per output group per point

### Boundedness

Bound the request path by:

- maximum range span
- maximum point count
- existing group-count guardrails
- daily bitmap row lookups only

## Worker Design

### Ownership

Append and repair continue to maintain `session_active_install_slices`.

The worker also owns:

- touched-day enqueue for active-install bitmap rebuilds
- daily bitmap rebuilds
- ordinal allocation for new installs

### Rebuild Rule

For each touched `(project_id, day)`:

1. load all active-install slice rows for that day
2. allocate missing ordinals
3. rebuild total/dim1/dim2 daily bitmaps from scratch for that day
4. replace prior rows transactionally

This keeps active-install maintenance incremental by day while avoiding
request-time distinct scans.

## Non-Goals

- no separate `/v1/metrics/active-installs` route
- no redesign of `count`, `duration_total`, or `new_installs`
- no compatibility alias for the old aligned-only `active_installs` contract
- no broad metrics-family rewrite beyond what `active_installs` itself needs

## Required Documentation Updates

Any implementation of this design must update:

- `schemas/openapi/fantasma.yaml`
- `docs/architecture.md`
- `docs/deployment.md`
- `docs/STATUS.md`
- CLI help and examples

## Acceptance Criteria

- `active_installs` stays on `GET /v1/metrics/sessions`
- exact `start`/`end` active-installs queries return truthful distinct-install
  values
- optional `interval=day|week|month|year` returns truthful clipped point windows
- grouped and filtered `active_installs` preserve existing D2 semantics
- public active-installs reads do not scan `sessions` with request-time
  `COUNT(DISTINCT ...)`
- week/month/year active-install cuboid maintenance is removed in favor of one
  daily bitmap storage model
- benchmark-sensitive read performance remains bounded

## Superseded Direction

This design supersedes the earlier March 17, 2026 aligned-range active-installs
design in
`docs/superpowers/specs/2026-03-17-range-active-installs-design.md` wherever
that older document implies that public `active_installs` should remain an
aligned bucket-only metric.
