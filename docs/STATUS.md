# Status

## Completed

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

- Run the end-to-end smoke test in a healthy local environment with Docker or a working local Postgres install.
- Add a dedicated migration workflow to replace startup bootstrap SQL once the raw schema stabilizes.
- Start the next slice after ingestion stabilizes: worker-owned session inference and aggregate generation.

## Open Decisions

- When to replace the temporary direct raw-event count query with worker-built aggregate reads.
- Which migration tool should own schema evolution after the bootstrap phase.
- Exact retention aggregation window strategy for reprocessing late-arriving events.
- First dashboard scope beyond API contract validation.
- Android SDK implementation schedule after the iOS SDK milestone.
