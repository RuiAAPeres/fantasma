# Status

## Active

- Local environment recovery: Docker Desktop content-store and Postgres data files are returning input/output errors in the current machine state, so the Compose smoke test for the new session slice needs to be rerun after disk space and Docker health are restored.

## Completed

- Implemented Fantasma's first derived metric: `fantasma-worker` now derives sessions from `events_raw`, stores them in `sessions`, tracks progress in `worker_offsets`, and exposes `GET /v1/metrics/sessions/count`, `GET /v1/metrics/sessions/duration`, and `GET /v1/metrics/active-installs`.
- Added the `sessions` table, true session upserts, the `(project_id, install_id, timestamp)` raw-event index, and a pure worker-side sessionization module with tests for inactivity splitting, install separation, user propagation, and bounded tail recompute behavior.
- Added DB-backed store and worker tests for session persistence, checkpoint round-trips, ordered install-window fetches, and tail-session updates.
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

- Restore local Docker/Postgres health, then rerun `docker compose -f infra/docker/compose.yaml up --build` and the full ingest -> worker -> sessions -> API smoke test in a healthy environment.
- Run the end-to-end iOS Simulator smoke test against the local stack and confirm the demo app drives both the raw-event and derived-session endpoints as expected.
- Add a dedicated migration workflow to replace startup bootstrap SQL once the raw schema stabilizes.
- Start the next aggregate slice after sessions stabilizes, likely screen views or release adoption.

## Open Decisions

- When to replace the temporary direct raw-event count query with worker-built aggregate reads.
- Which migration tool should own schema evolution after the bootstrap phase.
- Exact retention aggregation window strategy for reprocessing late-arriving events.
- First dashboard scope beyond API contract validation.
- Whether the next iOS SDK step should add richer property value types or keep string-only properties until the API surface expands deliberately.
- Android SDK implementation schedule after the iOS SDK milestone.
