# Status

## Completed

- Captured repository operating rules in `AGENTS.md`.
- Defined documentation-first workflow and project memory rules.
- Documented the initial Fantasma architecture and deployment targets.
- Bootstrapped the Cargo workspace and monorepo skeleton.
- Added shared event and metric domain types in `fantasma-core`.
- Added initial auth primitives and runnable ingest, API, and worker service binaries.

## In Progress

- Define the first public event and HTTP API contracts.
- Add Docker-based local deployment scaffolding.

## Next

- Introduce the initial OpenAPI contract for ingest and metric queries.
- Add event schema files under `schemas/events`.
- Add Docker Compose and service Dockerfiles for local bootstrapping.

## Open Decisions

- Exact retention aggregation window strategy for reprocessing late-arriving events.
- First dashboard scope beyond API contract validation.
- Android SDK implementation schedule after the iOS SDK milestone.
