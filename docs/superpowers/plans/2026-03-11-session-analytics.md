# Session Analytics Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build Fantasma's first worker-owned derived metric by inferring sessions from `events_raw`, storing them in `sessions`, and exposing session metrics through the public API.

**Architecture:** `fantasma-worker` polls raw events in batches, derives per-install sessions using a pure sessionization module, and upserts session rows plus a worker checkpoint. `fantasma-api` serves range metrics from the derived `sessions` table, while bootstrap SQL in `fantasma-store` owns the initial schema and indexes.

**Tech Stack:** Rust, Tokio, Axum, SQLx, Postgres, Docker Compose

---

## Chunk 1: Session derivation and persistence

### Task 1: Add failing tests for pure sessionization

**Files:**
- Create: `crates/fantasma-worker/src/sessionization.rs`
- Modify: `crates/fantasma-worker/src/main.rs`

- [ ] Write unit tests first for: same-session events, split sessions, separate installs, user propagation, deterministic session ids, tail recompute with prior session context.
- [ ] Run: `cargo test -p fantasma-worker sessionization -- --nocapture`
- [ ] Implement the pure sessionization module with no database access.
- [ ] Re-run: `cargo test -p fantasma-worker sessionization -- --nocapture`

### Task 2: Add failing store tests for session persistence

**Files:**
- Modify: `crates/fantasma-store/src/lib.rs`

- [ ] Add tests covering schema bootstrap, session upsert updates, checkpoint reads/writes, and install-specific raw event window queries.
- [ ] Run the new targeted store tests and confirm they fail for missing session support.
- [ ] Implement the new schema, indexes, and data-access helpers.
- [ ] Re-run the targeted store tests until green.

### Task 3: Implement worker polling and bounded tail recompute

**Files:**
- Modify: `crates/fantasma-worker/src/main.rs`
- Modify: `crates/fantasma-worker/Cargo.toml`

- [ ] Add a failing worker integration test path that inserts raw events, runs one worker batch, and asserts derived session rows.
- [ ] Verify the test fails before worker implementation.
- [ ] Implement batch polling, per-install grouping, timestamp-window tail recompute, real session upserts, and checkpoint advancement.
- [ ] Re-run the targeted worker tests until green.

## Chunk 2: Query API and documentation

### Task 4: Add failing API tests for session metrics

**Files:**
- Modify: `crates/fantasma-api/src/main.rs`
- Modify: `crates/fantasma-core/src/metrics.rs`

- [ ] Add tests for unauthorized requests, invalid time ranges, session count, average duration, and active installs.
- [ ] Run the targeted API tests and confirm failure.
- [ ] Implement the new query types, store calls, and route handlers.
- [ ] Re-run the targeted API tests until green.

### Task 5: Update OpenAPI and docs

**Files:**
- Modify: `schemas/openapi/fantasma.yaml`
- Modify: `docs/architecture.md`
- Modify: `docs/deployment.md`
- Modify: `docs/STATUS.md`

- [ ] Document the worker-owned session pipeline, bounded tail recompute behavior, and the three new public endpoints.
- [ ] Add local smoke-test steps that prove `SDK -> ingest -> events_raw -> worker -> sessions -> API`.

## Chunk 3: Verification

### Task 6: Full verification

**Files:**
- No code changes expected

- [ ] Run: `cargo test --workspace`
- [ ] Run: `cargo build --workspace`
- [ ] Run: `docker compose -f infra/docker/compose.yaml up --build`
- [ ] Execute the manual smoke flow for ingest, worker derivation, and session metric queries.
- [ ] Update `docs/STATUS.md` completion notes with actual verification status.
