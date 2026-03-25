# Hotspot 3 Ingest Fused Store Path Design

**Goal:** Reduce fixed ingest-path database round trips for `POST /v1/events` without changing the public API, auth ordering, validation behavior, or project-fencing correctness.

## Problem

The current ingest path in `crates/fantasma-ingest/src/http.rs` performs multiple store round trips before and during a successful insert:

1. `resolve_project_id_for_ingest_key()`
2. `load_project()`
3. parse and normalize the request body
4. `insert_events()`, which separately claims the ingest lease, inserts rows, verifies the fence, and commits

This shape preserves correctness, but it pays repeated lookup and transaction overhead on the hot path. The worker and aggregate work are correctly asynchronous already, so the remaining ingest hotspot is fixed DB overhead rather than synchronous aggregation.

## Constraints

- Keep the public `/v1/events` contract exactly the same.
- Preserve current auth-before-body-parse ordering.
- Preserve current response behavior for:
  - missing key -> `401 unauthorized`
  - invalid key -> `401 unauthorized`
  - revoked key -> `401 unauthorized`
  - wrong-kind key -> `401 unauthorized`
  - `range_deleting` project -> `409 project_busy`
  - `pending_deletion` project -> `409 project_pending_deletion`
  - invalid JSON -> `422 invalid_request`
  - validation errors -> current structured validation response
- Preserve ingest lease and fence verification semantics.
- Do not introduce synchronous aggregation, `COPY`, or broader auth subsystem refactors in this slice.

## Current Call Graph

### HTTP layer

`ingest_events()` currently:

- reads `x-fantasma-key`
- resolves the key to `project_id`
- loads the project row to inspect lifecycle state
- only then reads and validates the request body
- calls `insert_events()`

### Store layer

`insert_events()` currently:

- claims an ingest lease with `claim_project_processing_lease()`
- opens a second transaction for the insert
- batches inserts into `events_raw`
- verifies the lease fence
- commits
- releases the lease

This means one successful ingest batch pays:

- one key lookup query
- one project lookup query
- one lease-claim transaction
- one insert transaction
- one lease release query

## Recommended Design

### New fused store entrypoint

Add one new store function for accepted ingest batches, for example:

- `authenticate_and_insert_events(pool, ingest_key, events) -> Result<u64, StoreError>`

This function should:

1. resolve the supplied key from `api_keys`
2. reject revoked, missing, or wrong-kind keys as unauthorized
3. lock/load the target project row in the same transaction
4. gate on project lifecycle state before any insert work
5. claim the ingest project-processing lease in that same transaction
6. batch insert the accepted rows into `events_raw`
7. verify the lease fence before commit
8. commit
9. release the lease after the transactional work finishes

The HTTP handler should no longer call `resolve_project_id_for_ingest_key()` or `load_project()` directly on the hot success path.

### Handler behavior

`crates/fantasma-ingest/src/http.rs` should keep this order:

1. read `x-fantasma-key`
2. reject missing or malformed header as `401 unauthorized`
3. read and normalize the body only after the header is present and syntactically usable
4. call the fused store entrypoint with the normalized events
5. map typed store errors back to the exact current HTTP responses

Important: body parsing still must not run for callers that fail the header-presence / header-string gate.

### Store implementation shape

Prefer a transaction-local helper structure rather than stacking old public helpers:

- keep `resolve_api_key()` as a lower-level reusable primitive if still useful
- do not build the fused path by composing `resolve_project_id_for_ingest_key()` + `load_project()` + `insert_events()` unchanged
- instead, use one transaction that:
  - selects the key row
  - validates `kind = ingest`
  - joins or separately loads `projects ... FOR UPDATE`
  - inserts/refreshes the ingest lease row
  - performs the raw event insert
  - checks `fence_epoch`

This keeps the optimization real instead of hiding the same number of trips behind a new wrapper.

### Lease semantics

Keep the current ingest lease model intact:

- the ingest path still uses `ProjectProcessingActorKind::Ingest`
- fence verification still happens before commit
- release still happens after the transactional result is known

If a fence change or lifecycle transition is observed, the fused path must return the same error class the HTTP handler already maps to `project_busy`, `project_pending_deletion`, or `unauthorized`.

## Error Model

The fused store function should return the existing `StoreError` variants whenever possible so the HTTP layer can preserve behavior with minimal new branching:

- unauthorized cases:
  - `ProjectNotFound` only when the key maps nowhere valid by the time the transactional work runs
  - a new explicit unauthorized variant is acceptable if it simplifies wrong-kind / revoked / missing classification internally
- busy cases:
  - `ProjectNotActive(ProjectState::RangeDeleting)`
  - `ProjectNotActive(ProjectState::PendingDeletion)`
  - `ProjectFenceChanged`

If a new `StoreError` variant is introduced for auth classification, it must remain internal-facing and map back to the exact current `401 unauthorized` response.

## Non-Goals

- No public API changes
- No SDK changes
- No event contract changes
- No raw table schema changes
- No `COPY`
- No broader refactor of non-ingest API key paths

## Tests

### Ingest HTTP coverage

Add or extend tests covering:

- missing `x-fantasma-key` -> `401`
- malformed header value -> `401`
- invalid key -> `401`
- revoked key -> `401`
- wrong-kind key against `POST /v1/events` -> `401`
- `range_deleting` project -> `409 project_busy`
- `pending_deletion` project -> `409 project_pending_deletion`
- invalid JSON -> `422 invalid_request`
- validation errors -> structured validation response
- success -> `202 accepted`
- auth-before-body-parse ordering on unauthorized requests

### Store coverage

Add tests for the fused store path covering:

- active ingest key inserts rows successfully
- wrong-kind key is rejected
- revoked key is rejected
- missing key is rejected
- lifecycle gating returns the expected `StoreError`
- fence change during ingest returns the expected `StoreError`
- insert count matches accepted event count

## Benchmark Gate

Before and after implementation, run:

- `live-append-small-blobs`
- `live-append-plus-light-repair`

Primary acceptance target:

- representative append throughput should improve or remain flat
- checkpoint and final derived readiness must not regress materially

This slice is specifically about fixed ingest overhead, so representative append is the most important benchmark signal.

## Docs

Update:

- `docs/STATUS.md` when the slice starts and finishes
- `docs/architecture.md` if the ingest call path description changes enough to warrant explicit documentation
