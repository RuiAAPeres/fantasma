# Project And Range Deletion API Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add operator-only full-project purge and range-delete APIs with explicit project lifecycle state, independent deletion-job history, fenced project-scoped mutation coordination, CLI coverage, and SDK blocking semantics.

**Architecture:** Extend the `projects` table with explicit lifecycle state plus a fencing epoch, add independent `project_deletions` and `project_processing_leases` tables, and route all project-scoped mutating flows through a shared active-state/lease guard. Full-project purge remains an explicit store-owned delete path over every project-owned table; range delete uses a fenced transactional delete-and-rebuild path that commits state restoration and job success atomically or rolls back and records failure cleanly.

**Tech Stack:** Rust (`fantasma-api`, `fantasma-ingest`, `fantasma-store`, `fantasma-worker`, `fantasma-cli`), Swift (`FantasmaSDK`), SQL migrations, OpenAPI YAML, architecture/deployment/SDK docs.

---

## Chunk 1: Persistence model and project state

### Task 1: Add store-level failing tests for project lifecycle state and deletion-job persistence

**Files:**
- Modify: `crates/fantasma-store/src/lib.rs`

- [ ] **Step 1: Add failing migration/schema tests for `projects.state`, `projects.fence_epoch`, `project_deletions`, and `project_processing_leases`**
- [ ] **Step 2: Add failing store tests for loading/listing projects with lifecycle state and for deletion-job history surviving project-row removal**
- [ ] **Step 3: Run the focused `fantasma-store` tests and confirm they fail for the expected missing schema/helpers**

### Task 2: Add the new schema and store primitives

**Files:**
- Create: `crates/fantasma-store/migrations/0020_project_deletions_and_fencing.sql`
- Modify: `crates/fantasma-store/src/lib.rs`

- [ ] **Step 1: Add the migration for project lifecycle state, independent deletion jobs, and processing leases**
- [ ] **Step 2: Extend `ProjectRecord` and related row decoders to include lifecycle state and fence epoch**
- [ ] **Step 3: Add store helpers for lifecycle transitions, deletion-job CRUD/listing, lease claim/release/waiting, and historical project-id lookups**
- [ ] **Step 4: Re-run the focused `fantasma-store` tests and confirm green**

## Chunk 2: Management API contract

### Task 3: Add failing API tests for the deletion endpoints and lifecycle-state responses

**Files:**
- Modify: `crates/fantasma-api/tests/auth.rs`
- Modify: `crates/fantasma-api/src/http.rs`

- [ ] **Step 1: Add failing route tests for `DELETE /v1/projects/{project_id}`, `POST /v1/projects/{project_id}/deletions`, `GET /v1/projects/{project_id}/deletions`, and `GET /v1/projects/{project_id}/deletions/{deletion_id}`**
- [ ] **Step 2: Add failing tests that project list/get responses now include lifecycle state and that key routes honor the historical-row contract**
- [ ] **Step 3: Add failing tests for idempotent purge behavior, historical job lookup, and mismatched `project_id` / `deletion_id` returning `404`**
- [ ] **Step 4: Run the focused API auth/route tests and confirm red**

### Task 4: Implement the management API and operator JSON surface

**Files:**
- Modify: `crates/fantasma-api/src/http.rs`
- Modify: `crates/fantasma-store/src/lib.rs`

- [ ] **Step 1: Add request parsing and response JSON for range-delete scope and deletion-job records**
- [ ] **Step 2: Add the four deletion routes and wire their idempotency / historical lookup behavior through store helpers**
- [ ] **Step 3: Add project lifecycle state to management list/get payloads**
- [ ] **Step 4: Re-run the focused API tests and confirm green**

## Chunk 3: State gating for ingest, reads, and management mutations

### Task 5: Add failing gating tests before mutating behavior

**Files:**
- Modify: `crates/fantasma-ingest/tests/http.rs`
- Modify: `crates/fantasma-api/tests/auth.rs`

- [ ] **Step 1: Add failing ingest tests for `409 project_busy` during `range_deleting` and `409 project_pending_deletion` during `pending_deletion`**
- [ ] **Step 2: Add failing API tests for read-key route blocking, rename/key-create/key-revoke blocking, and keys-list `404` after project-row removal**
- [ ] **Step 3: Run the focused ingest and API tests to confirm red**

### Task 6: Implement shared project-state gating across ingest and API reads/writes

**Files:**
- Modify: `crates/fantasma-ingest/src/http.rs`
- Modify: `crates/fantasma-api/src/http.rs`
- Modify: `crates/fantasma-store/src/lib.rs`

- [ ] **Step 1: Resolve project state alongside key auth so ingest and read routes can return the required `409` envelopes**
- [ ] **Step 2: Gate project rename, key create, and key revoke behind `active` state only**
- [ ] **Step 3: Preserve allowed management reads and keys-list behavior while the project row still exists**
- [ ] **Step 4: Re-run the focused ingest/API tests and confirm green**

## Chunk 4: Fencing and worker/delete execution

### Task 7: Add failing worker/store tests for fence sequencing and delete execution semantics

**Files:**
- Modify: `crates/fantasma-store/src/lib.rs`
- Modify: `crates/fantasma-worker/src/worker.rs`
- Modify: `crates/fantasma-worker/tests/pipeline.rs`

- [ ] **Step 1: Add failing store/worker tests that stale mutating work cannot commit after the fence epoch advances**
- [ ] **Step 2: Add failing tests for full purge idempotency, purge failure staying `pending_deletion`, and successful purge leaving historical jobs queryable after project-row removal**
- [ ] **Step 3: Add failing tests for range delete transactional success/failure semantics, zero-match success, and rebuild coverage across touched installs**
- [ ] **Step 4: Run the focused worker/store pipeline tests and confirm red**

### Task 8: Implement leases, fencing, full purge, and range-delete rebuild execution

**Files:**
- Modify: `crates/fantasma-store/src/lib.rs`
- Modify: `crates/fantasma-worker/src/worker.rs`
- Modify: `crates/fantasma-worker/src/scheduler.rs`
- Modify: `crates/fantasma-worker/src/lib.rs`

- [ ] **Step 1: Add shared store helpers for mutating-flow lease claim/release, epoch verification, and delete-worker job claiming**
- [ ] **Step 2: Thread the active-state/lease rule through ingest-adjacent mutations, session apply, session repair, event metrics, project patch, and key mutations**
- [ ] **Step 3: Add worker handling for queued deletion jobs, including fence wait, transactional range delete + rebuild, and explicit ordered full purge**
- [ ] **Step 4: Ensure range-delete success commits raw deletes, rebuilds, job success, and project reactivation together; ensure failure marks the job failed and restores `active` in a fresh follow-up transaction**
- [ ] **Step 5: Ensure purge success updates the independent deletion job before deleting the project row in the same transaction; ensure purge failure records failed while keeping `pending_deletion`**
- [ ] **Step 6: Re-run the focused worker/store pipeline tests and confirm green**

## Chunk 5: CLI operator workflow

### Task 9: Add failing CLI tests for deletion commands and lifecycle-aware output

**Files:**
- Modify: `crates/fantasma-cli/tests/http_flows.rs`

- [ ] **Step 1: Add failing CLI tests for project purge, range-delete creation, deletion-job list/get, and lifecycle state in project output**
- [ ] **Step 2: Run the focused CLI HTTP-flow tests and confirm red**

### Task 10: Implement CLI deletion commands

**Files:**
- Modify: `crates/fantasma-cli/src/cli.rs`
- Modify: `crates/fantasma-cli/src/app.rs`

- [ ] **Step 1: Add operator command shapes for purge, range-delete creation, deletion listing, and deletion lookup**
- [ ] **Step 2: Add request/response DTOs plus text/JSON rendering for lifecycle state and deletion-job records**
- [ ] **Step 3: Keep the CLI as the primary operator workflow by routing all new project-deletion actions through these commands**
- [ ] **Step 4: Re-run the focused CLI tests and confirm green**

## Chunk 6: iOS SDK blocking semantics

### Task 11: Add failing SDK tests for `project_busy` vs `project_pending_deletion`

**Files:**
- Modify: `sdks/ios/FantasmaSDK/Tests/FantasmaSDKTests/FantasmaSDKTests.swift`

- [ ] **Step 1: Add failing tests that `409 project_busy` keeps rows queued without latching the destination**
- [ ] **Step 2: Add failing tests that `409 project_pending_deletion` keeps rows queued, latches the destination blocked, stops automatic uploads, and only clears on changed destination signature**
- [ ] **Step 3: Run the focused Swift tests and confirm red**

### Task 12: Implement SDK blocked-destination behavior without widening the public error API

**Files:**
- Modify: `sdks/ios/FantasmaSDK/Sources/FantasmaSDK/EventUploader.swift`
- Modify: `sdks/ios/FantasmaSDK/Sources/FantasmaSDK/SQLiteQueue.swift`
- Modify: `sdks/ios/FantasmaSDK/Sources/FantasmaSDK/FantasmaCore.swift`

- [ ] **Step 1: Decode upload `409` responses internally so `project_busy` stays transient and `project_pending_deletion` latches the destination**
- [ ] **Step 2: Persist the blocked latch alongside the existing destination signature so restarts preserve the stop-retrying behavior**
- [ ] **Step 3: Keep public `flush()` and failed uploads on the existing `uploadFailed` surface**
- [ ] **Step 4: Re-run the focused Swift tests and confirm green**

## Chunk 7: Contract/docs alignment and verification

### Task 13: Update OpenAPI, architecture, deployment, SDK docs, and project memory

**Files:**
- Modify: `schemas/openapi/fantasma.yaml`
- Modify: `docs/architecture.md`
- Modify: `docs/deployment.md`
- Modify: `sdks/ios/FantasmaSDK/README.md`
- Modify: `docs/STATUS.md`

- [ ] **Step 1: Document the new project lifecycle state, deletion routes, request/response schemas, and operator/SDK behavior**
- [ ] **Step 2: Record both the start and completion of the slice in `docs/STATUS.md` with fresh verification evidence**

### Task 14: Run impacted verification

**Files:**
- Modify: none

- [ ] **Step 1: Run focused `fantasma-store`, `fantasma-api`, `fantasma-ingest`, `fantasma-worker`, `fantasma-cli`, and Swift SDK tests covering the new contract**
- [ ] **Step 2: Run `cargo fmt --all --check`**
- [ ] **Step 3: Run `cargo clippy --workspace --all-targets -- -D warnings`**
- [ ] **Step 4: Run `bash -n scripts/cli-smoke.sh` and extend `./scripts/cli-smoke.sh` if the new operator workflow needs smoke coverage**
- [ ] **Step 5: Summarize exact verification evidence and any remaining follow-up work before handoff**
