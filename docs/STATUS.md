# Status

## Active

- No active implementation work recorded. See `Completed` and `Next` for the latest shipped slice and follow-up work.

## Engineering Ethos

- Fantasma prefers simple, explicit, high-performance data paths.
- We avoid steady-state designs that rely on broad recomputation.
- We narrow feature shape when necessary to preserve predictable performance and operational clarity.
- Correctness still matters, but we should choose correctness models that compose with incremental processing.

## Completed

- Refocused the deployment docs around actual stack bring-up and operator verification: `docs/deployment.md` now documents the Compose topology, runtime configuration, startup behavior, preconditions, bring-up, smoke flow, and manual health checks, while architecture/query-policy text and contributor test workflow details were removed from the deployment runbook and the iOS demo walkthrough moved to `apps/demo-ios/README.md`.
- Hardened the performance-proof slice after review: `fantasma-bench` now scopes Docker commands to the dedicated `fantasma-bench` Compose project, benchmark services moved to isolated host ports so local runs cannot tear down the default dev stack, startup cleanup now fails closed instead of benchmarking against dirty state, the worker catch-up regression now proves at least one grouped event-metrics request ran during concurrent catch-up, and the performance docs now point at the real worker test command.
- Fixed a consistency bug in the event-metrics query path: grouped event-metrics reads now run inside one repeatable-read snapshot instead of stitching multiple cube selects across live worker commits, and a new DB-backed pipeline test locks that regression down by hammering `GET /v1/metrics/events/daily` while the event-metrics worker catches up one batch at a time.
- Recalibrated the enforced CI benchmark budgets from retained GitHub `ubuntu-latest` artifacts on March 12, 2026. Retained medians came in at `12.35k` events/s and `523ms` ready for `hot-path`, `11.47k` / `13.00k` events/s with `347.5ms` / `342ms` readiness for `repair-path`, and `11.51k` events/s with `13.56s` readiness for `scale-path`; readiness/query ceilings now still use `20%` median slack, but they never tighten below the slowest retained runner sample.
- Added performance proofing for Fantasma's bounded backend paths: PR CI now locks the worker fanout boundary and benchmark Compose config, the repository has a first-class `fantasma-bench` harness plus `infra/docker/compose.bench.yaml`, and a dedicated `performance.yml` workflow now runs numeric hot-path and repair-path stack benchmarks on `main` and manual dispatch.
- Clarified the event-property cap across public docs: `README.md` now states only the high-level principle that Fantasma keeps event context narrow for predictable performance, while the event schema, OpenAPI, and architecture docs carry the exact 3-key limit and its worker-aggregate rationale.
- Refocused `README.md` into a product-facing entry point instead of a contract dump, and tightened `AGENTS.md` so future contributors keep detailed implementation semantics in deployment, architecture, SDK, and schema docs instead of pushing them back into the README.
- Followed up on the docs-centralization pass by removing repeated `install_id` policy prose from deployment, architecture, and SDK docs. `README.md` stays the canonical explanation; derived docs now focus on local contract or behavior instead of re-explaining the same privacy boundary.
- Centralized Fantasma's public direction after the install-scoped event-metrics work: `README.md` now acts as the canonical product/privacy statement, `AGENTS.md` tells contributors not to spread that stance across multiple docs, and deployment, architecture, SDK, OpenAPI, and event-schema text now describe the same install-scoped/eventually-consistent contract instead of restating it with drift.
- Refined the event-metrics dim2/dim3 read path after review: forward migration `0007_event_metrics_read_indexes.sql` replaces the older slot-ordered indexes with key/value-aware index coverage so canonical later-slot filters still stay on a bounded indexed path, and store migration tests now lock that index set in place.
- Tightened the worker-derived event-metrics slice after review: group-limit failures now run through bounded aggregate prechecks before the full daily synthesis path, migration history is forward-only again via `0006_remove_legacy_identity_columns.sql`, and the published OpenAPI now documents structural ingest `invalid_request` responses, event-metrics `500` envelopes, and the explicit-property filter namespace.
- Replaced the raw event-count route with worker-derived event metrics: `GET /v1/metrics/events/aggregate` and `GET /v1/metrics/events/daily` now read from bounded daily Postgres rollups, support explicit filtering and grouping, synthesize null buckets, hard-fail on group overflows, and use inclusive UTC `start_date` / `end_date` semantics.
- Added the event-metrics rollup storage slice in Postgres with forward migration `0005_event_metrics_rollups.sql`, `os_version` on `events_raw`, worker-owned `event_count_daily_total` plus 1/2/3-dimension cuboids, database-enforced canonical slot ordering, and end-to-end worker processing in both tests and the long-running worker service.
- Narrowed the public event contract to explicit string-only properties capped at 3 keys, added built-in `os_version`, preserved indexed ingest validation through raw DTO normalization, updated JSON Schema/OpenAPI/deployment docs, and updated the iOS SDK docs/tests/examples so supported clients auto-populate `platform`, `app_version`, and `os_version`.
- Removed public `user_id` and client-sent `session_id` from Fantasma's MVP contract, rewrote the undeployed SQL migration history to the install-scoped shape, kept backend session IDs internal-only, removed `identify(_:)` from the iOS SDK, and updated docs/examples/scripts to match.
- Updated the Compose smoke flow to reset its disposable Postgres volume before and after each run so schema rewrites do not leave stale migration history behind.
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
- Relaxed the initial event contract to support string `install_id`, optional `app_version`, smaller batch limits, and payload-size guardrails.
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

- Let the calibrated `Performance` workflow run on `main` after merge so default-branch history starts with the tightened benchmark budgets instead of the branch-only calibration samples.
- Run the end-to-end iOS Simulator smoke test against the local stack and confirm the demo app drives the new worker-derived event metrics alongside the existing session metrics.
- Decide whether the next aggregate slice should extend event metrics to more product-facing use cases such as release adoption or screen-view dashboards, while preserving the same bounded incremental model.

## Open Decisions

- Whether future late-event handling should stay bounded to per-install exact-day repair or eventually add a separate explicitly scoped backfill workflow.
- First dashboard scope beyond API contract validation.
- Android SDK implementation schedule after the iOS SDK milestone.
