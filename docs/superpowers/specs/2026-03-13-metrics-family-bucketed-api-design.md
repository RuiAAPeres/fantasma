# Metrics Family Bucketed API Design

## Goal

Replace Fantasma's current fan-out metrics API with two family endpoints that
serve only worker-derived bucketed series for `hour` and `day`.

## Approved Public Contract

Fantasma exposes exactly two metrics endpoints:

- `GET /v1/metrics/events`
- `GET /v1/metrics/sessions`

Both endpoints require:

- `metric`
- `granularity`
- `start`
- `end`

Time semantics:

- `granularity=day` uses `start=YYYY-MM-DD` and `end=YYYY-MM-DD`
- `granularity=hour` uses UTC RFC3339 hour-start timestamps
- both ranges are inclusive and UTC-defined
- `day` rejects timestamps
- `hour` rejects non-hour-aligned timestamps

Response shape is uniform for both families:

- always include `metric`
- always include `granularity`
- always include `group_by`
- always include `series`
- ungrouped responses use `group_by: []`
- ungrouped responses use one series item with `dimensions: {}`

## Family Semantics

### Events

`GET /v1/metrics/events` supports:

- `metric=count`
- `granularity=hour|day`
- bounded built-in filters on `platform`, `app_version`, `os_version`
- bounded explicit property equality filters
- bounded `group_by`

Existing privacy and bounded-read rules stay in force:

- do not expose install-level filtering or grouping
- keep explicit-property filtering explicit
- keep the total referenced-dimension count bounded
- keep group-limit failures explicit
- legacy public params such as `start_date`, `end_date`, and `project_id` must
  fail fast and must not be treated as property filters

### Sessions

`GET /v1/metrics/sessions` supports:

- `metric=count`
- `metric=duration_total`
- `metric=new_installs`
- `granularity=hour|day`
- bounded `group_by` and equality filters on `platform` and `app_version` only

Unsupported session dimensions or filters return `422`.

Session grouping semantics:

- `platform` and `app_version` come from the session's first event
- later events in the same session do not rebucket grouped rollups
- late-event repair preserves the existing grouped assignment for already-derived sessions

`duration_total` semantics:

- assign the full session duration to the bucket where the session starts
- bucket choice follows the requested `hour` or `day` granularity in UTC
- later tail extension updates that start bucket only

`new_installs` semantics:

- an install is counted exactly once
- bucket assignment comes from the first accepted event Fantasma sees
- late events never retroactively move that install into an earlier bucket

## Worker And Storage Requirements

The implementation may extend existing structures or introduce new ones. The
internal physical layout is not part of the public contract.

Required capabilities:

- worker-derived `hour` and `day` event rollups/cuboids sufficient to serve the
  new `/v1/metrics/events` contract
- worker-derived `hour` and `day` session rollups sufficient to serve
  `/v1/metrics/sessions` for `count`, `duration_total`, and `new_installs`
- bounded grouped reads for sessions on `platform` and `app_version`
- repeatable-read event query snapshots where multiple bounded reads must agree
- durable first-seen install state for fixed `new_installs` assignment
- bounded repair that touches only affected session-derived buckets
- no synchronous aggregation in ingest
- no broad steady-state recomputation
- Postgres-only operation

## Validation And Failure Modes

The rewritten contract should fail clearly when:

- `metric` is missing or unsupported
- `granularity` is missing or unsupported
- `start` or `end` is missing, malformed, or out of order
- `hour` requests are not hour aligned
- legacy public params appear
- unsupported session dimensions or filters appear
- grouped reads exceed the configured group limit

## Repository Scope

This work is complete only when the new contract is reflected consistently in:

- Rust code and tests
- OpenAPI
- architecture and deployment docs
- smoke scripts
- benchmark or harness code
- `docs/STATUS.md`
