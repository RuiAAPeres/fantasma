# Status

## Active

- No active implementation work is recorded in this snapshot.

## Engineering Ethos

- Fantasma prefers simple, explicit, high-performance data paths.
- We avoid steady-state designs that rely on broad recomputation.
- We narrow feature shape when necessary to preserve predictable performance and operational clarity.
- Correctness still matters, but we should choose correctness models that compose with incremental processing.

## Completed

- Implemented Fantasma's first derived metric: `fantasma-worker` now derives sessions from `events_raw`, stores them in `sessions`, tracks progress in `worker_offsets`, and exposes `GET /v1/metrics/sessions/count`, `GET /v1/metrics/sessions/duration`, and `GET /v1/metrics/active-installs`.
- Replaced startup bootstrap DDL with `sqlx` migrations in `crates/fantasma-store/migrations/` and split schema preparation into always-run migrations plus optional local project seeding.
- Added bounded exact-day historical repair to the tail-state worker: out-of-order install batches now recompute only the overlapping derived sessions, keep the read/delete/write sequence inside one transaction, and rebuild only the affected UTC session-start days.
- Added `session_daily_installs` so daily active installs are maintained from explicit membership state instead of `COUNT(DISTINCT ...)` rebuilds.
- Kept daily metrics incremental and Postgres-only: `session_daily` still stores UTC `DATE` buckets for `sessions_count`, `active_installs`, and `total_duration_seconds`, with the hot append path updated incrementally and the repair path rebuilding only exact touched days.
- Exposed explicit daily-series endpoints for `GET /v1/metrics/sessions/count/daily` and `GET /v1/metrics/sessions/duration/total/daily` with inclusive `start_date` / `end_date` query semantics and zero-filled UTC series responses.
- Extracted reusable library entrypoints for ingest, API, and worker batch processing so in-process end-to-end tests can drive `POST /v1/events`, run one worker batch, and query daily metrics without spawning services.
- Added a preflighted Compose smoke script that checks disk and Docker availability, ingests sample events, polls the daily metrics endpoint, and dumps logs on timeout.
- Recorded the repository preference that DB-backed Rust tests should run fully in Docker, with workspace Postgres tests separated from stack-level smoke verification.
- Added a dedicated Docker test workflow with `infra/docker/compose.test.yaml`, `infra/docker/Dockerfile.test`, and `./scripts/docker-test.sh` so DB-backed Rust tests can run fully inside Docker against containerized Postgres using `sqlx::test`, without requiring host-user writes to the Cargo cache volumes.
- Added DB-backed store and worker tests for tail state persistence, bounded out-of-order repair, exact-day daily rebuilds, checkpoint round-trips, incremental daily metric updates, and daily install-membership tracking.
- Updated OpenAPI, architecture, deployment docs, and CI so the repository documents and tests the `events_raw -> worker -> sessions -> metrics API` slice.
- Updated `AGENTS.md` to align Fantasma's agent workflow with the installed `superpowers` skills, including expectations for planning, debugging, verification, subagent execution, and review.
- Implemented the first iOS SDK prototype as a Swift Package with explicit `configure`, `track`, `identify`, `flush`, and `clear` APIs, a durable SQLite-backed event queue, and asynchronous uploads to `POST /v1/events`.
- Added Swift tests covering event serialization, queue persistence, queue replay after failures, batch deletion rules, and identity rotation semantics.
- Added a demo iOS app that exercises the SDK against the local development ingest service with explicit `app_open`, `screen_view`, and `button_pressed` events.
- Implemented vertical slice 1: `POST /v1/events` now validates batches, resolves project-scoped ingest keys from Postgres, and inserts raw events into `events_raw`.
- Added startup Postgres bootstrap for `projects`, `api_keys`, and `events_raw`, including raw-event indexes for project/time, install, event name, platform, and received time.
- Added `GET /v1/metrics/events/count` with bearer admin auth and direct counting from `events_raw`.
- Relaxed the initial event contract to support string `install_id`, optional `session_id`, optional `app_version`, smaller batch limits, and payload-size guardrails.
- Updated local deployment defaults, OpenAPI, JSON schemas, and architecture/deployment docs for the first end-to-end ingest slice.
- Captured repository operating rules in `AGENTS.md`.
- Defined documentation-first workflow and project memory rules.
- Documented the initial Fantasma architecture and deployment targets.
- Bootstrapped the Cargo workspace and monorepo skeleton.
- Added shared event and metric domain types in `fantasma-core`.
- Added initial auth primitives and runnable ingest, API, and worker service binaries.
- Added the first public OpenAPI contract and event schema files.
- Added Docker-based local deployment scaffolding and a dashboard placeholder.
- Added GitHub Actions CI for formatting, linting, tests, schema JSON validation, and Compose config checks.

## Next

- Run the end-to-end iOS Simulator smoke test against the local stack and confirm the demo app drives both the raw-event and derived-session endpoints as expected.
- Start the next aggregate slice only if it can follow the same explicit incremental-processing model, likely screen views or release adoption.

## Open Decisions

- When to replace the temporary direct raw-event count query with worker-built aggregate reads.
- Whether future late-event handling should stay bounded to per-install exact-day repair or eventually add a separate explicitly scoped backfill workflow.
- First dashboard scope beyond API contract validation.
- Whether the next iOS SDK step should add richer property value types or keep string-only properties until the API surface expands deliberately.
- Android SDK implementation schedule after the iOS SDK milestone.
