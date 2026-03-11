# Architecture

## Principles

- Everything is an event.
- Events are immutable.
- Aggregation is asynchronous.
- APIs are public by default.
- SDK behavior must be explicit.
- Deployment must stay simple.

## High-Level Flow

```text
curl / SDK
  -> POST /v1/events
  -> Rust ingest service
  -> append-only Postgres events_raw table
  -> GET /v1/metrics/events/count
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
- populate aggregate tables for product metrics
- keep processing idempotent
- support version-aware mobile analytics

Planned aggregates:

- daily active users
- sessions by day
- screen views by day
- retention cohorts
- version adoption
- custom event counts by day

### API Service

Responsibilities:

- expose project-scoped query endpoints
- read raw events directly for the first vertical slice
- provide a stable public API for first-party and third-party clients

## Data Model Direction

Core persisted concepts:

- `projects`
- `api_keys`
- `events_raw`
- aggregate tables keyed by project, metric window, and dimensions

Event requirements:

- `event`
- `timestamp`
- `install_id`
- `platform`

Optional event fields:

- `session_id`
- `user_id`
- `app_version`
- small `properties` JSON object

## SDK Direction

The first SDK target is iOS.

Required client behavior:

- `configure(serverURL, writeKey)`
- `track(eventName, properties?)`
- `identify(userId)`
- `flush()`
- `clear()`

Durability requirement:

- persist each event before attempting upload
- use local SQLite for the queue
- delete only acknowledged rows
- preserve queued events across `clear()`

Current iOS SDK shape:

- expose a single static `Fantasma` facade backed by one shared client
- store `install_id`, `user_id`, and current `session_id` in local defaults
- serialize each tracked event to JSON and store it as an immutable SQLite row
- upload queued events asynchronously to `POST /v1/events` using the existing batch contract

Current upload triggers:

- every 10 seconds
- when the local queue reaches 50 events
- when `flush()` is called
- when the app enters background

Current identity rules:

- `install_id` is generated on first use and reused until `clear()`
- `identify(userId)` applies only to future events
- `clear()` rotates `install_id`, clears `user_id`, rotates `session_id`, and leaves already queued rows untouched
