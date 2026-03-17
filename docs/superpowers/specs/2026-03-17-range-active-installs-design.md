# Range Active Installs API Design

## Goal

Refocus Fantasma's public metrics API on the highest-value product metrics by
cutting low-value event-volume work and adding truthful range-based
`active_installs` buckets.

## Product Direction

Fantasma should optimize for metrics that help operators understand product
reach, acquisition, and engagement, not instrumentation volume.

In this slice:

- `active_installs` across meaningful time buckets is the priority.
- `new_installs` remains a core acquisition metric.
- `sessions` and `duration_total` remain useful primitives.
- `average_duration` is a dashboard-derived metric, not a backend primitive.
- total event volume is not important enough to justify additional API or
  storage complexity.

## Decisions

### Keep

- `GET /v1/metrics/events?metric=count&event=...`
- `GET /v1/metrics/sessions?metric=count`
- `GET /v1/metrics/sessions?metric=duration_total`
- `GET /v1/metrics/sessions?metric=new_installs`
- `GET /v1/metrics/sessions?metric=active_installs`
- Existing D2 filter and `group_by` contract where already supported.

### Cut

- Remove `GET /v1/metrics/events/total`.
- Do not add `metric=total_count` to `/v1/metrics/events`.
- Do not add any backend `average_duration` metric.
- Do not expand `/v1/metrics/events/top` or `/v1/metrics/events/catalog` in
  this slice.

### Add

Extend `GET /v1/metrics/sessions?metric=active_installs` beyond day-level only,
with truthful distinct-install buckets for:

- `granularity=day`
- `granularity=week`
- `granularity=month`
- `granularity=year` only if it fits cleanly in the same bounded design

If `year` materially complicates the implementation or benchmark envelope, stop
at `day/week/month` in this slice.

## Public API Contract

### Route Surface

After this slice, the public metrics routes remain:

- `GET /v1/metrics/events`
- `GET /v1/metrics/events/catalog`
- `GET /v1/metrics/events/top`
- `GET /v1/metrics/sessions`

`GET /v1/metrics/events/total` is removed with no compatibility alias.

### Sessions Metrics

Supported public session metrics remain:

- `count`
- `duration_total`
- `new_installs`
- `active_installs`

### `active_installs` Contract

`active_installs` is a distinct-install metric. It reports the number of unique
installs active within each returned bucket.

Supported granularities:

- `day`
- `week`
- `month`
- `year` if implemented cleanly in this slice

Supported filters and grouping:

- built-ins: `platform`, `app_version`, `os_version`
- explicit first-event session properties
- `group_by` up to 2 dimensions
- total referenced dimensions across filters plus `group_by` capped at 2

Truthfulness rules:

- week/month/year values must be true distinct installs in that bucket
- week/month/year must not be derived by summing daily active installs
- grouped or filtered slices may overlap where the metric semantics already
  allow overlap

Response shape:

- unchanged from the existing metrics family
- ungrouped reads still return `group_by: []` and `dimensions: {}`

### `average_duration`

`average_duration` is not a backend metric.

Clients should derive it from:

- `duration_total`
- `count`

## Architecture Constraints

### Worker-Derived Only

The new `active_installs` range support must remain worker-derived.

Do not implement range-based `active_installs` via:

- request-time distinct scans over raw events
- request-time distinct scans over large session ranges
- general arbitrary-window distinct logic

### Bounded Scope

This slice should add fixed truthful bucket shapes only. It must not introduce a
general "active installs over any arbitrary window" metric.

The intended product surface is:

- DAI-like daily active installs
- WAI/WAU-like weekly active installs
- MAI/MAU-like monthly active installs
- optional yearly active installs only if bounded cleanly

### Performance Priority

This design must preserve Fantasma's benchmark envelope.

If a design choice materially threatens append throughput, checkpoint freshness,
or predictable bounded read paths, narrow the feature shape rather than widening
the architecture.

## Non-Goals

- no `metric=total_count`
- no compatibility alias for `/v1/metrics/events/total`
- no backend `average_duration`
- no arbitrary-window active-install metric
- no expansion of event discovery endpoints for this slice

## Required Documentation Updates

Any implementation of this design must update:

- `schemas/openapi/fantasma.yaml`
- `docs/architecture.md`
- `docs/deployment.md`
- `docs/STATUS.md`
- CLI help and validation if operator-facing session metrics are exposed there

## Acceptance Criteria

- `/v1/metrics/events/total` no longer exists.
- `active_installs` supports truthful range buckets for the implemented fixed
  granularities.
- week/month/year `active_installs` values are not computed by summing daily
  buckets.
- existing session D2 filtering and grouping behavior remains intact for the
  supported `active_installs` granularities.
- no backend `average_duration` metric is introduced.
- benchmark-sensitive implementation stays bounded and worker-derived.
