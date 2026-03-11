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
Mobile SDK
  -> POST /v1/events
  -> Rust ingest service
  -> append-only Postgres events table
  -> worker-driven aggregate tables
  -> query API
  -> dashboard or external clients
```

## Services

### Ingest Service

Responsibilities:

- authenticate project-scoped ingest keys
- validate incoming event batches
- normalize SDK metadata
- batch insert raw events into Postgres
- acknowledge accepted and rejected events

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
- read precomputed aggregate tables
- provide a stable public API for first-party and third-party clients

## Data Model Direction

Core persisted concepts:

- `projects`
- `ingest_keys`
- `admin_tokens`
- `events`
- aggregate tables keyed by project, metric window, and dimensions

Event requirements:

- `event`
- `timestamp`
- `install_id`
- `session_id`
- `platform`
- `app_version`

Optional event fields:

- `user_id`
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
