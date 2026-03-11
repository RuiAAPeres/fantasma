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

## In Progress

- Add GitHub Actions CI for workspace and deployment checks.

## Next

- Validate CI locally and commit it as a dedicated slice.
- Start replacing bootstrap auth and response stubs with database-backed behavior.

## Open Decisions

- Exact retention aggregation window strategy for reprocessing late-arriving events.
- First dashboard scope beyond API contract validation.
- Android SDK implementation schedule after the iOS SDK milestone.
