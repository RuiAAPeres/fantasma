# Fantasma Public Project And Event Discovery Design

## Goal

Add the narrow public API routes the private dashboard needs without introducing any hosted-product concepts into Fantasma. The new surface should cover project metadata reads/edits under operator auth and lightweight event discovery reads under read-key auth.

## Scope

This slice adds:

- `GET /v1/projects/{project_id}`
- `PATCH /v1/projects/{project_id}`
- `GET /v1/metrics/events/catalog`
- `GET /v1/metrics/events/top`

It also includes the required Fantasma updates around:

- route auth-kind enforcement
- request validation
- OpenAPI
- docs
- regression tests

This slice does not add:

- workspace or subscription concepts
- dashboard-specific query shapes
- browser-oriented backdoors
- broad exploratory query APIs

## Route Boundaries

Fantasma keeps the existing split between management and analytics reads:

- `GET /v1/projects/{project_id}` and `PATCH /v1/projects/{project_id}` are management routes and require operator bearer auth.
- `GET /v1/metrics/events/catalog` and `GET /v1/metrics/events/top` are analytics routes and require project-scoped `read` keys.

The management routes operate on a caller-supplied `project_id`.

The analytics routes stay project-scoped from the authenticated read key and must not accept public `project_id` query parameters.

## Request And Response Shapes

### GET /v1/projects/{project_id}

Response:

```json
{
  "project": {
    "id": "uuid",
    "name": "Fantasma Demo iOS",
    "created_at": "2026-03-15T12:00:00Z"
  }
}
```

### PATCH /v1/projects/{project_id}

Request:

```json
{
  "name": "New Project Name"
}
```

Response uses the same `project` shape as the GET route.

Validation:

- `project_id` must be a UUID path id.
- `name` must reuse the same project-name validation used by project creation.
- unknown fields are rejected

### GET /v1/metrics/events/catalog

Query:

- `start`
- `end`
- optional supported dimension filters already accepted by event metrics parsing

Response:

```json
{
  "events": [
    { "name": "app_open", "last_seen_at": "2026-03-15T11:45:00Z" },
    { "name": "button_pressed", "last_seen_at": "2026-03-15T11:40:00Z" }
  ]
}
```

Ordering:

- `last_seen_at` descending
- `name` ascending as deterministic tie-breaker

### GET /v1/metrics/events/top

Query:

- `start`
- `end`
- `limit`
- optional supported dimension filters already accepted by event metrics parsing

Response:

```json
{
  "events": [
    { "name": "app_open", "count": 1823 },
    { "name": "screen_view", "count": 944 }
  ]
}
```

Ordering:

- `count` descending
- `name` ascending as deterministic tie-breaker

Validation:

- the same public time-window rules as existing event metrics routes
- the same invalid filter-key rejection model as existing event metrics routes
- `limit` defaults to `10`
- `limit` rejects values below `1` or above `50`

## Implementation Boundaries

### crates/fantasma-api

Owns:

- route wiring
- auth-kind enforcement
- path/query parsing
- response shaping
- OpenAPI assertions

### crates/fantasma-store

Owns:

- fetching one project by id
- patching a project name
- listing distinct event names plus last-seen timestamps in a bounded window
- listing top events plus counts in a bounded window

The store API should stay narrow and use the existing project-scoped raw/aggregate data model rather than introducing dashboard-specific tables or query abstractions.

## Query Strategy

Project metadata reads and writes should use the existing `projects` table.

The event discovery routes should read from raw events, scoped by authenticated `project_id`, because they are discovery helpers rather than bucketed chart series and they need distinct event names across a bounded time range. Optional filters should stay limited to the same event/public dimension vocabulary already accepted by `GET /v1/metrics/events`.

That keeps the routes accurate even when the caller has not specified one concrete event name and avoids overloading the aggregate event-count endpoints with discovery-only semantics.

## Error Handling

- missing or invalid auth returns `401`
- missing project returns `404`
- invalid UUID path ids return `404` through Axum path parsing behavior or `400`/`422` if existing Fantasma route conventions require explicit validation handling
- invalid patch body returns `422`
- invalid analytics query keys or ranges reuse the current event metrics error codes
- internal store failures return `500`

## Test Plan

Required coverage:

- operator auth required for project metadata routes
- read-key auth required for catalog/top routes
- operator bearer token rejected on catalog/top
- read key rejected on project metadata routes
- missing project returns `404`
- patch rejects unknown fields
- patch persists renamed project metadata
- catalog returns distinct event names in deterministic order
- top returns ranked counts in deterministic order
- invalid query keys, invalid `limit`, and public `project_id` query params are rejected
- OpenAPI includes the four new routes with the correct security declarations

## Documentation

Update:

- `schemas/openapi/fantasma.yaml`
- `docs/architecture.md`
- `docs/deployment.md` if the management/query boundary description changes materially
- `docs/STATUS.md`

`README.md` only needs an update if the high-level public surface summary materially changes.
