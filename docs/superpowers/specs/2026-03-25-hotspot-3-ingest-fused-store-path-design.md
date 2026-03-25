# Hotspot 3 Ingest Fused Store Path Design

**Goal:** Reduce fixed ingest-path database round trips for `POST /v1/events` without changing the public API, auth-before-body-read behavior, validation behavior, or project-fencing correctness.

## Problem

The current ingest path in `crates/fantasma-ingest/src/http.rs` performs multiple store round trips before and during a successful insert:

1. `resolve_project_id_for_ingest_key()`
2. `load_project()`
3. parse and normalize the request body
4. `insert_events()`, which separately claims the ingest lease, inserts rows, verifies the fence, and commits

This shape preserves correctness, but it pays repeated lookup and transaction overhead on the hot path. The worker and aggregate work are correctly asynchronous already, so the remaining ingest hotspot is fixed DB overhead rather than synchronous aggregation.

## Constraints

- Keep the public `/v1/events` contract exactly the same.
- Preserve the current auth-and-project-state gate before the request body is read.
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

### Revised fused-path shape

Because the current handler fully authenticates the ingest key and gates on project state before it reads the body, a single store call that only runs after body parse is not compatible with the current public behavior.

For this slice, preserve the current ordering and use two store-side phases:

1. a pre-body-read auth/state gate that fuses:
   - ingest key lookup
   - revoked / wrong-kind / missing-key rejection
   - project lifecycle gating
2. a post-parse insert path that fuses:
   - ingest lease claim
   - raw-event batch insert
   - fence verification

This is the maximum safe fusion that keeps the externally observable request ordering unchanged.

### New pre-parse store entrypoint

Add a store function for the auth/state gate, for example:

- `authenticate_ingest_request(pool, ingest_key) -> Result<AuthenticatedIngestProject, StoreError>`

This function should:

1. resolve the supplied key from `api_keys`
2. reject revoked, missing, or wrong-kind keys as unauthorized
3. load the target project row in the same query or transaction
4. gate on project lifecycle state before the handler reads the body
5. return a small authenticated ingest context containing at least `project_id`

The HTTP handler should no longer call `resolve_project_id_for_ingest_key()` and `load_project()` separately on the hot path.

### New post-parse fused insert entrypoint

Add a second store function for accepted ingest batches, for example:

- `insert_events_with_authenticated_ingest(pool, auth, events) -> Result<u64, StoreError>`

This function should:

1. start one transaction
2. load current project state and fence in that transaction
3. claim the ingest project-processing lease in that same transaction
4. batch insert the accepted rows into `events_raw`
5. verify the lease fence before commit
6. commit
7. release the lease after the transactional work finishes

### Handler behavior

`crates/fantasma-ingest/src/http.rs` should keep this order:

1. read `x-fantasma-key`
2. reject missing or malformed header as `401 unauthorized`
3. call the new pre-parse auth/state gate
4. only if that succeeds, read and normalize the body
5. call the post-parse fused insert entrypoint with the authenticated ingest context and normalized events
6. map typed store errors back to the exact current HTTP responses

Important: body parsing must not run for callers that fail either the header-presence check or the auth/state gate.

### Store implementation shape

Prefer transaction-local helpers rather than stacking old public helpers:

- keep `resolve_api_key()` as a lower-level reusable primitive if still useful
- do not build the new path by composing `resolve_project_id_for_ingest_key()` + `load_project()` + `insert_events()` unchanged
- the pre-parse auth/state gate should collapse the current key lookup plus lifecycle read into one store entrypoint
- the post-parse insert path should collapse lease claim, insert, and fence verification into one transaction
- do not require `FOR UPDATE` on the project row in the success path unless profiling or a concrete race demonstrates it is necessary

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
- body-read ordering proof for all pre-parse gate failures:
  - unauthorized requests
  - `range_deleting` projects
  - `pending_deletion` projects

### Store coverage

Add tests for the fused store path covering:

- active ingest key authenticates successfully
- wrong-kind key is rejected
- revoked key is rejected
- missing key is rejected
- active authenticated ingest context inserts rows successfully
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
