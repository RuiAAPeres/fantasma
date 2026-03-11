# Status

## Completed

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

- Start replacing bootstrap auth and response stubs with database-backed behavior.
- Add initial Postgres schema and migrations for projects, keys, tokens, and raw events.
- Replace stub metric responses with aggregate-backed query paths.

## Open Decisions

- Exact retention aggregation window strategy for reprocessing late-arriving events.
- First dashboard scope beyond API contract validation.
- Android SDK implementation schedule after the iOS SDK milestone.
