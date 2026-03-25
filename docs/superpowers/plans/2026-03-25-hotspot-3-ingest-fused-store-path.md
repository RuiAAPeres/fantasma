# Hotspot 3 Ingest Fused Store Path Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce fixed `POST /v1/events` database round trips while preserving the current public API, auth-before-body-read behavior, and ingest lease correctness.

**Architecture:** Split the optimization into two internal store phases. First, replace separate ingest-key resolution and project lifecycle lookup with one pre-parse auth/state gate. Second, replace the current insert path with one post-parse transactional helper that claims the ingest lease, inserts rows, verifies the fence, and commits. Keep the HTTP handler’s observable behavior unchanged and prove that with targeted ingest tests before changing production code.

**Tech Stack:** Rust, Axum, SQLx, Postgres, Docker-backed integration tests, `fantasma-ingest`, `fantasma-store`

---

## File Map

- Modify: `crates/fantasma-ingest/src/http.rs`
  Handler wiring for the new pre-parse auth/state gate and post-parse fused insert path.
- Modify: `crates/fantasma-ingest/tests/http.rs`
  HTTP regressions for unauthorized, revoked, wrong-kind, lifecycle-gated, invalid-body, validation, and success paths, including body-read ordering proof.
- Modify: `crates/fantasma-store/src/lib.rs`
  New fused store entrypoints, typed ingest auth context, and store-level tests for auth classification, lifecycle gating, lease/fence handling, and insert behavior.
- Modify: `docs/STATUS.md`
  Start/finish notes plus benchmark results.
- Modify: `docs/architecture.md`
  Only if the final internal ingest call path merits explicit architecture documentation.

## Chunk 1: Lock The Desired Behavior With Failing Tests

### Task 1: Extend ingest HTTP coverage for the new pre-parse gate

**Files:**
- Modify: `crates/fantasma-ingest/tests/http.rs`

- [ ] **Step 1: Add failing HTTP regressions for auth/state gate behavior**

Add tests for:
- invalid key -> `401`
- revoked key -> `401`
- wrong-kind key -> `401` (keep existing one if already sufficient)
- `range_deleting` project -> `409 project_busy`
- `pending_deletion` project -> `409 project_pending_deletion`
- body-read ordering proof for:
  - unauthorized request
  - `range_deleting` request
  - `pending_deletion` request

For the body-read ordering proof, use a streaming request body that would fail if the handler attempted to read it, and assert the request still returns the pre-parse gate response.

- [ ] **Step 2: Run the focused ingest HTTP tests and verify the new ones fail for the right reason**

Run:
```bash
FANTASMA_DOCKER_TEST_KEEP_CACHE=1 ./scripts/docker-test.sh -p fantasma-ingest --test http --quiet
```

Expected:
- existing tests may pass
- at least the new ordering/auth coverage fails until the new store path is wired in

- [ ] **Step 3: Commit the red HTTP test expansion**

```bash
git add crates/fantasma-ingest/tests/http.rs
git commit -m "Add ingest auth gate regressions"
```

### Task 2: Add failing store tests for the fused ingest helpers

**Files:**
- Modify: `crates/fantasma-store/src/lib.rs`

- [ ] **Step 1: Add failing store tests for the new pre-parse auth/state gate**

Add `#[sqlx::test]` coverage for:
- active ingest key authenticates successfully
- invalid key is rejected
- revoked key is rejected
- wrong-kind key is rejected
- `range_deleting` project returns `StoreError::ProjectNotActive(ProjectState::RangeDeleting)`
- `pending_deletion` project returns `StoreError::ProjectNotActive(ProjectState::PendingDeletion)`

- [ ] **Step 2: Add failing store tests for the new post-parse fused insert path**

Add coverage for:
- authenticated ingest context inserts the expected row count
- fence change causes `StoreError::ProjectFenceChanged`
- non-active project state during insert maps to the expected `StoreError`

- [ ] **Step 3: Run the focused store tests and verify they fail correctly**

Run:
```bash
FANTASMA_DOCKER_TEST_KEEP_CACHE=1 ./scripts/docker-test.sh -p fantasma-store --lib --quiet
```

Expected:
- the new fused ingest helper tests fail because the helpers do not exist yet

- [ ] **Step 4: Commit the red store tests**

```bash
git add crates/fantasma-store/src/lib.rs
git commit -m "Add fused ingest store regressions"
```

## Chunk 2: Implement The Store Helpers

### Task 3: Implement the pre-parse auth/state gate

**Files:**
- Modify: `crates/fantasma-store/src/lib.rs`

- [ ] **Step 1: Add a small authenticated ingest context type**

Create a small internal/public-as-needed struct that carries at least:
- `project_id`
- enough information to support the post-parse insert helper without redoing key classification logic

- [ ] **Step 2: Implement `authenticate_ingest_request(...)`**

Requirements:
- resolve the API key
- reject missing, revoked, and wrong-kind keys
- gate on project lifecycle state
- avoid introducing `FOR UPDATE` in the success path unless needed
- return typed `StoreError` values that the HTTP layer can map back to current responses

- [ ] **Step 3: Run the focused store tests**

Run:
```bash
FANTASMA_DOCKER_TEST_KEEP_CACHE=1 ./scripts/docker-test.sh -p fantasma-store --lib --quiet
```

Expected:
- auth/state gate tests now pass
- insert-path tests may still fail

- [ ] **Step 4: Commit the auth/state helper**

```bash
git add crates/fantasma-store/src/lib.rs
git commit -m "Fuse ingest auth and project gate"
```

### Task 4: Implement the post-parse fused insert helper

**Files:**
- Modify: `crates/fantasma-store/src/lib.rs`

- [ ] **Step 1: Implement `insert_events_with_authenticated_ingest(...)`**

Requirements:
- one transaction for state/fence read, lease claim, insert, and fence verification
- preserve `ProjectProcessingActorKind::Ingest`
- release the lease after transactional completion
- keep the old `insert_events()` around only if still needed; otherwise route it through the new helper or remove it cleanly

- [ ] **Step 2: Run the focused store tests again**

Run:
```bash
FANTASMA_DOCKER_TEST_KEEP_CACHE=1 ./scripts/docker-test.sh -p fantasma-store --lib --quiet
```

Expected:
- new fused ingest store tests pass

- [ ] **Step 3: Commit the fused insert helper**

```bash
git add crates/fantasma-store/src/lib.rs
git commit -m "Fuse ingest lease and insert path"
```

## Chunk 3: Wire The HTTP Handler

### Task 5: Move `POST /v1/events` onto the new store path

**Files:**
- Modify: `crates/fantasma-ingest/src/http.rs`
- Modify: `crates/fantasma-ingest/tests/http.rs`

- [ ] **Step 1: Update the handler to use the pre-parse auth/state gate**

Change the handler flow to:
- read header
- reject missing / malformed header
- call `authenticate_ingest_request(...)`
- only then read and normalize the body

- [ ] **Step 2: Update the handler to use the post-parse fused insert helper**

Use the authenticated ingest context from the pre-parse gate and map errors back to the exact current HTTP responses.

- [ ] **Step 3: Run the ingest HTTP suite**

Run:
```bash
FANTASMA_DOCKER_TEST_KEEP_CACHE=1 ./scripts/docker-test.sh -p fantasma-ingest --test http --quiet
```

Expected:
- all ingest HTTP tests pass

- [ ] **Step 4: Commit the HTTP wiring**

```bash
git add crates/fantasma-ingest/src/http.rs crates/fantasma-ingest/tests/http.rs
git commit -m "Use fused ingest store path"
```

## Chunk 4: Full Verification, Benchmarks, And Cleanup

### Task 6: Run the required verification matrix

**Files:**
- Modify: `docs/STATUS.md`
- Modify: `docs/architecture.md` (only if needed)

- [ ] **Step 1: Run formatting, core tests, clippy, and affected Docker suites**

Run:
```bash
cargo fmt --all --check
cargo test -p fantasma-core --quiet
cargo clippy --workspace --all-targets -- -D warnings
FANTASMA_DOCKER_TEST_KEEP_CACHE=1 ./scripts/docker-test.sh -p fantasma-store --lib --quiet
FANTASMA_DOCKER_TEST_KEEP_CACHE=1 ./scripts/docker-test.sh -p fantasma-ingest --test http --quiet
git diff --check
```

Expected:
- all commands exit 0

- [ ] **Step 2: Run before/after append benchmarks**

Run:
```bash
FANTASMA_ADMIN_TOKEN=test-token cargo run -p fantasma-bench -- slo --output-dir artifacts/performance/2026-03-25-hotspot-3-before --scenario live-append-small-blobs --scenario live-append-plus-light-repair
FANTASMA_ADMIN_TOKEN=test-token cargo run -p fantasma-bench -- slo --output-dir artifacts/performance/2026-03-25-hotspot-3-after --scenario live-append-small-blobs --scenario live-append-plus-light-repair
```

Expected:
- representative append is improved or flat
- checkpoint/final derived readiness is not materially worse

- [ ] **Step 3: Update project memory and architecture docs as needed**

Record:
- exact benchmark deltas
- verification commands run
- whether any old ingest helpers were removed or left as compatibility wrappers

- [ ] **Step 4: Make the final implementation commit**

```bash
git add docs/STATUS.md docs/architecture.md
git commit -m "Document Hotspot 3 ingest results"
```

- [ ] **Step 5: Discard churn**

Before handoff:
- confirm only intended source/docs changes are staged or committed
- do not stage old benchmark artifacts or unrelated exploratory files
- remove only new throwaway files created during this slice if they are not part of the kept result
