# Status

## Completed

- Captured repository operating rules in `AGENTS.md`.
- Defined documentation-first workflow and project memory rules.
- Documented the initial Fantasma architecture and deployment targets.

## In Progress

- Bootstrap the Rust workspace and monorepo skeleton.
- Define the first public event and HTTP API contracts.

## Next

- Add Cargo workspace members for core, auth, ingest, worker, and API crates.
- Introduce the initial OpenAPI contract for ingest and metric queries.
- Add compilable service skeletons and shared domain types.

## Open Decisions

- Exact retention aggregation window strategy for reprocessing late-arriving events.
- First dashboard scope beyond API contract validation.
- Android SDK implementation schedule after the iOS SDK milestone.
