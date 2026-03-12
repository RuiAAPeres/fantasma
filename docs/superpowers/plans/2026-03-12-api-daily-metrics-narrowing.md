# Daily Metrics API Narrowing Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Narrow Fantasma's daily metrics API surface to only session count and total session duration, and remove `active_installs_daily` from the API contract, docs, tests, and smoke flow.

**Architecture:** Keep the existing shared daily-series response shape and `session_daily` backing store, but reduce the public route set to the two incrementally maintained metrics the redesign still supports. Update API-side tests first, then make the minimal route/schema/doc changes and refresh repository memory in `docs/STATUS.md`.

**Tech Stack:** Rust, Axum, serde, OpenAPI YAML, Markdown docs, Bash smoke script

---

### Task 1: Prove the public route surface is too wide

**Files:**
- Modify: `crates/fantasma-api/src/http.rs`

- [ ] Write a failing API-side test that proves:
  - `GET /v1/metrics/sessions/count/daily` is routable.
  - `GET /v1/metrics/sessions/duration/total/daily` is routable.
  - `GET /v1/metrics/active-installs/daily` is not routable.
- [ ] Run only that test and confirm it fails because the old route still exists.

### Task 2: Narrow the implementation and contract

**Files:**
- Modify: `crates/fantasma-api/src/http.rs`
- Modify: `schemas/openapi/fantasma.yaml`
- Modify: `docs/architecture.md`
- Modify: `docs/deployment.md`
- Modify: `docs/STATUS.md`
- Modify: `AGENTS.md`
- Modify: `scripts/compose-smoke.sh`

- [ ] Remove `active_installs_daily` from the API route set and contract.
- [ ] Update docs and smoke flow to mention only the surviving daily metrics.
- [ ] Add the exact engineering ethos text once the exact wording is available.

### Task 3: Verify and hand off

**Files:**
- Modify: `docs/STATUS.md`

- [ ] Run targeted verification for `fantasma-api`.
- [ ] Report changed files, verification results, and any remaining blocker.
