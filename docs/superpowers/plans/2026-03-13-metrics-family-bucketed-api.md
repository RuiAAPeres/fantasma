# Metrics Family Bucketed API Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace Fantasma's legacy metrics fan-out with two worker-derived family endpoints that serve uniform hourly and daily series for events and sessions.

**Architecture:** Keep the public contract strict and compact while leaving internal storage layout flexible. Add failing tests first, then implement shared bucketed types, collapsed API routing/query parsing, worker-derived hourly and daily rollups, durable first-seen install state for `new_installs`, and finally align schemas, docs, smoke, and harnesses.

**Tech Stack:** Rust, Axum, sqlx/Postgres, serde, OpenAPI YAML, Markdown docs, Bash smoke script

---

### Task 1: Lock the new public contract in failing tests

**Files:**
- Modify: `crates/fantasma-api/src/http.rs`
- Modify: `crates/fantasma-api/tests/auth.rs`
- Modify: `crates/fantasma-worker/tests/pipeline.rs`

- [ ] Add failing route-contract tests proving only `/v1/metrics/events` and `/v1/metrics/sessions` are routable.
- [ ] Add failing parser/response tests for required `metric`, `granularity`, `start`, and `end`.
- [ ] Add failing tests for the unified response shape, including `group_by: []` and `dimensions: {}` for ungrouped responses.
- [ ] Add failing tests that `start_date`, `end_date`, and `project_id` return `422` and are never reinterpreted as event-property filters.
- [ ] Run the focused tests and confirm they fail for the expected contract reasons.

### Task 2: Add failing worker/store tests for new rollup semantics

**Files:**
- Modify: `crates/fantasma-store/src/lib.rs`
- Modify: `crates/fantasma-worker/src/worker.rs`
- Modify: `crates/fantasma-worker/tests/pipeline.rs`

- [ ] Add failing tests for hourly event rollups and hourly session rollups.
- [ ] Add failing tests for grouped session reads on `platform` and `app_version`, plus `422` rejection of unsupported session dimensions.
- [ ] Add failing tests for fixed first-seen `new_installs`.
- [ ] Add failing tests for `duration_total` staying assigned to the session-start bucket.
- [ ] Run the focused tests and confirm they fail before implementation.

### Task 3: Implement shared types and collapsed API routing

**Files:**
- Modify: `crates/fantasma-core/src/metrics.rs`
- Modify: `crates/fantasma-core/src/lib.rs`
- Modify: `crates/fantasma-core/src/events.rs`
- Modify: `crates/fantasma-api/src/http.rs`

- [ ] Introduce shared bucketed metric query/response types.
- [ ] Reserve the new public query keys and keep legacy public params failing fast.
- [ ] Collapse the public routing to `/v1/metrics/events` and `/v1/metrics/sessions`.
- [ ] Implement strict query parsing and uniform response shaping for both families.
- [ ] Keep event group-limit and null-bucket behavior intact under the new contract.

### Task 4: Implement worker-derived hourly and daily rollups

**Files:**
- Modify: `crates/fantasma-store/migrations/*`
- Modify: `crates/fantasma-store/src/lib.rs`
- Modify: `crates/fantasma-worker/src/worker.rs`

- [ ] Add storage and helpers sufficient to serve worker-derived event and session metrics for `hour` and `day`.
- [ ] Add durable first-seen install state for `new_installs`.
- [ ] Update the event worker to maintain hourly and daily event rollups.
- [ ] Update the session worker to maintain hourly and daily session rollups, keep session-start dimensions fixed, and keep repair bounded.
- [ ] Implement bounded rebuild paths for touched session-derived buckets without retroactive `new_installs` rebucketing.

### Task 5: Align repository surfaces

**Files:**
- Modify: `schemas/openapi/fantasma.yaml`
- Modify: `docs/architecture.md`
- Modify: `docs/deployment.md`
- Modify: `docs/STATUS.md`
- Modify: `scripts/compose-smoke.sh`
- Modify: `crates/fantasma-bench/src/main.rs`
- Modify: `apps/demo-ios/README.md`

- [ ] Rewrite OpenAPI to the two-endpoint family contract.
- [ ] Update docs, smoke flow, and harnesses to the new query model and response shape.
- [ ] Remove lingering references to legacy paths, legacy metrics, or mixed time models.

### Task 6: Verify and record completion

**Files:**
- Modify: `docs/STATUS.md`

- [ ] Run targeted verification for API, store, worker, smoke, and bench/harness surfaces touched by the rewrite.
- [ ] Add a completion entry to `docs/STATUS.md` with the final contract and verification evidence.
- [ ] Report changed files, verification results, and any remaining risk.
