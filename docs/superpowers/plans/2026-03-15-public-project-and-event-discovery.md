# Public Project And Event Discovery Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add operator-auth project metadata routes and read-key event discovery routes to Fantasma with tests, OpenAPI, and docs.

**Architecture:** Extend `fantasma-api` with two new management routes and two new analytics routes, backed by narrow `fantasma-store` queries. Keep auth-kind separation explicit: operator bearer auth for `/v1/projects/{project_id}` management reads/writes and read-key auth for `/v1/metrics/events/catalog` and `/v1/metrics/events/top`. Reuse existing event metrics filter and time-window validation patterns instead of inventing a parallel query model.

**Tech Stack:** Rust, Axum, SQLx, Postgres, OpenAPI YAML, Docker-backed sqlx tests

---

## Chunk 1: Spec, Store Contracts, And Project Metadata Routes

### Task 1: Add failing store and API tests for project metadata routes

**Files:**
- Modify: `crates/fantasma-api/tests/auth.rs`
- Modify: `crates/fantasma-api/src/http.rs`
- Modify: `crates/fantasma-store/src/lib.rs`

- [ ] **Step 1: Write the failing auth and behavior tests**

Add tests to `crates/fantasma-api/tests/auth.rs` that prove:

- `GET /v1/projects/{project_id}` requires operator bearer auth
- `PATCH /v1/projects/{project_id}` requires operator bearer auth
- read keys are rejected on both project metadata routes
- `GET /v1/projects/{project_id}` returns the created project payload
- `PATCH /v1/projects/{project_id}` updates the project name and returns the updated payload
- `PATCH` rejects unknown fields
- missing project ids return `404`

- [ ] **Step 2: Run only the new auth tests to verify they fail**

Run:
- `cargo test -p fantasma-api --test auth project_metadata`

Expected: FAIL because the routes and store helpers do not exist yet.

- [ ] **Step 3: Add narrow store helpers**

In `crates/fantasma-store/src/lib.rs`, add:

- `fetch_project(pool_or_tx, project_id)`
- `rename_project(pool_or_tx, project_id, name)`

Both should return `Option<ProjectRecord>` and reuse the existing `projects` table shape.

- [ ] **Step 4: Add the metadata routes**

In `crates/fantasma-api/src/http.rs`:

- wire `GET /v1/projects/{project_id}`
- wire `PATCH /v1/projects/{project_id}`
- add a `PatchProjectRequest` with `#[serde(deny_unknown_fields)]`
- reuse the same project-name validation used by project creation
- return `{ "project": ... }`

- [ ] **Step 5: Re-run the focused auth tests**

Run:
- `cargo test -p fantasma-api --test auth project_metadata`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/fantasma-api/tests/auth.rs crates/fantasma-api/src/http.rs crates/fantasma-store/src/lib.rs
git commit -m "feat: add project metadata routes"
```

## Chunk 2: Event Discovery Reads

### Task 2: Add failing tests for event catalog and top-events routes

**Files:**
- Modify: `crates/fantasma-api/tests/auth.rs`
- Modify: `crates/fantasma-worker/tests/pipeline.rs`
- Modify: `crates/fantasma-api/src/http.rs`
- Modify: `crates/fantasma-store/src/lib.rs`

- [ ] **Step 1: Write the failing route/auth tests**

Extend `crates/fantasma-api/tests/auth.rs` with tests that prove:

- `GET /v1/metrics/events/catalog` requires a read key
- `GET /v1/metrics/events/top` requires a read key
- operator bearer auth is rejected on both routes
- public `project_id` query params are rejected on both routes
- invalid `limit` values are rejected on top-events

- [ ] **Step 2: Write the failing integration tests**

Extend `crates/fantasma-worker/tests/pipeline.rs` with tests that ingest real events and prove:

- catalog returns distinct event names with deterministic ordering by last-seen desc then name asc
- top-events returns ranked counts with deterministic ordering by count desc then name asc
- optional supported filters scope both outputs correctly

- [ ] **Step 3: Run the focused tests to verify they fail**

Run:
- `cargo test -p fantasma-api --test auth event_discovery`
- `cargo test -p fantasma-worker --test pipeline event_discovery`

Expected: FAIL because the routes and store queries do not exist.

- [ ] **Step 4: Add narrow store queries**

In `crates/fantasma-store/src/lib.rs`, add:

- `list_event_catalog(...)`
- `list_top_events(...)`

They should:

- scope by `project_id`
- honor the same time-window and supported filter vocabulary as public event metrics
- query raw events directly
- return narrow typed records with `name + last_seen_at` or `name + count`

- [ ] **Step 5: Add query parsing and route handlers**

In `crates/fantasma-api/src/http.rs`:

- add query structs/parsers for catalog and top-events
- reuse existing event metrics query validation patterns for `start`, `end`, and filters
- add `limit` parsing with default `10` and max `50`
- add handlers for `/v1/metrics/events/catalog` and `/v1/metrics/events/top`
- keep auth-kind enforcement aligned with existing read-key metrics routes

- [ ] **Step 6: Re-run the focused tests**

Run:
- `cargo test -p fantasma-api --test auth event_discovery`
- `cargo test -p fantasma-worker --test pipeline event_discovery`

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/fantasma-api/tests/auth.rs crates/fantasma-worker/tests/pipeline.rs crates/fantasma-api/src/http.rs crates/fantasma-store/src/lib.rs
git commit -m "feat: add event discovery routes"
```

## Chunk 3: OpenAPI, Docs, And Final Verification

### Task 3: Update OpenAPI, docs, and regression assertions

**Files:**
- Modify: `schemas/openapi/fantasma.yaml`
- Modify: `crates/fantasma-api/src/http.rs`
- Modify: `docs/STATUS.md`
- Modify: `docs/architecture.md`
- Modify: `docs/deployment.md`

- [ ] **Step 1: Add failing OpenAPI/doc assertions**

Extend existing spec assertions in `crates/fantasma-api/src/http.rs` tests to require:

- the four new paths
- management vs metrics security declarations
- top-events `limit` parameter docs

- [ ] **Step 2: Run the focused API tests to verify they fail**

Run:
- `cargo test -p fantasma-api openapi`

Expected: FAIL because the OpenAPI file and assertions are not updated yet.

- [ ] **Step 3: Update OpenAPI and docs**

Add the new routes, parameters, security, and response schemas to `schemas/openapi/fantasma.yaml`.

Update:

- `docs/architecture.md` with the expanded public API surface
- `docs/deployment.md` if needed for operator/read-key boundary clarity
- `docs/STATUS.md` with current state and next steps

- [ ] **Step 4: Re-run the focused API tests**

Run:
- `cargo test -p fantasma-api openapi`

Expected: PASS.

- [ ] **Step 5: Run full verification**

Run:
- `cargo fmt --all --check`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo test -p fantasma-api --test auth`
- `cargo test -p fantasma-worker --test pipeline`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add schemas/openapi/fantasma.yaml crates/fantasma-api/src/http.rs docs/STATUS.md docs/architecture.md docs/deployment.md
git commit -m "docs: publish project and event discovery routes"
```

- [ ] **Step 7: Push**

```bash
git push origin main
```
