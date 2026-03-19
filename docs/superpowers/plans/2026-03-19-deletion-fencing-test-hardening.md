# Deletion Fencing Test Hardening Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Strengthen deletion and fencing coverage so scheduler selection, stale/fresh lease behavior, historical route behavior, and operator CLI workflows are locked down by regressions.

**Architecture:** Add tests at the narrowest layer that proves each contract. Use store tests for claimant/lease selection semantics, API tests for historical and key-route behavior, and CLI tests for operator workflows and request shape. Prefer direct DB assertions for scheduler semantics and end-to-end HTTP assertions for route/CLI behavior.

**Tech Stack:** Rust, `sqlx::test`, Axum in-process routers, `fantasma-cli` integration tests, Docker-backed Postgres tests

---

## Chunk 1: Store Selection And Lease Predicate Tests

### Task 1: Lock down lease freshness semantics

**Files:**
- Modify: `crates/fantasma-store/src/lib.rs`

- [ ] **Step 1: Write the failing test**
  Add a store test that creates one fresh older-epoch lease and one stale older-epoch lease for the same project, then asserts `count_project_processing_leases_before_epoch()` counts only the fresh lease.

- [ ] **Step 2: Run test to verify it fails or exposes a gap**

Run: `cargo test -p fantasma-store count_project_processing_leases_ignores_stale_leases -- --exact`

- [ ] **Step 3: Write minimal implementation if needed**
  Adjust lease-counting behavior only if the test exposes a real mismatch.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p fantasma-store count_project_processing_leases_ignores_stale_leases -- --exact`

### Task 2: Lock down deletion claimant fairness

**Files:**
- Modify: `crates/fantasma-store/src/lib.rs`

- [ ] **Step 1: Write the failing test**
  Add a store test that creates a blocked deletion job for project A plus a runnable queued deletion for project B, then asserts `claim_next_project_deletion_job_in_tx()` returns project B’s job.

- [ ] **Step 2: Run test to verify it fails or exposes a gap**

Run: `cargo test -p fantasma-store claim_next_project_deletion_job_skips_blocked_projects -- --exact`

- [ ] **Step 3: Write minimal implementation if needed**
  Adjust only claimant selection logic if the test still exposes a mismatch.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p fantasma-store claim_next_project_deletion_job_skips_blocked_projects -- --exact`

## Chunk 2: API Historical And Key-Route Contract Tests

### Task 3: Historical successful purge contract

**Files:**
- Modify: `crates/fantasma-api/tests/auth.rs`

- [ ] **Step 1: Write the failing test**
  Add an API test proving `DELETE /v1/projects/{id}` returns the latest successful purge job after the project row is already gone.

- [ ] **Step 2: Run test to verify it fails or exposes a gap**

Run: `cargo test -p fantasma-api delete_project_returns_latest_successful_historical_purge -- --exact`

- [ ] **Step 3: Write minimal implementation if needed**
  Fix only route/store wiring if the test finds a mismatch.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p fantasma-api delete_project_returns_latest_successful_historical_purge -- --exact`

### Task 4: Historical keys and mismatched deletion lookup contract

**Files:**
- Modify: `crates/fantasma-api/tests/auth.rs`

- [ ] **Step 1: Write the failing tests**
  Add API tests proving:
  1. `GET /v1/projects/{id}/keys` returns `404` once the project row is gone even if deletion history remains.
  2. `GET /v1/projects/{project_id}/deletions/{deletion_id}` returns `404` when the deletion belongs to a different project.

- [ ] **Step 2: Run tests to verify they fail or expose gaps**

Run: `cargo test -p fantasma-api historical_keys_return_not_found_after_project_row_removal -- --exact`
Run: `cargo test -p fantasma-api deletion_get_returns_not_found_for_mismatched_project -- --exact`

- [ ] **Step 3: Write minimal implementation if needed**
  Fix only route/store behavior if the tests expose mismatches.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p fantasma-api historical_keys_return_not_found_after_project_row_removal -- --exact`
Run: `cargo test -p fantasma-api deletion_get_returns_not_found_for_mismatched_project -- --exact`

## Chunk 3: Operator CLI Workflow Tests

### Task 5: CLI purge/list/get workflow

**Files:**
- Modify: `crates/fantasma-cli/tests/http_flows.rs`

- [ ] **Step 1: Write the failing tests**
  Extend the test server with deletion endpoints and add CLI tests proving:
  1. `fantasma projects delete` uses the active project and operator token.
  2. `fantasma projects deletions list` uses the active project and renders deletion metadata.
  3. `fantasma projects deletions get <id>` targets the correct project-scoped route.

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p fantasma-cli --test http_flows projects_delete_uses_active_project -- --exact`
Run: `cargo test -p fantasma-cli --test http_flows project_deletions_list_uses_active_project -- --exact`
Run: `cargo test -p fantasma-cli --test http_flows project_deletions_get_uses_active_project -- --exact`

- [ ] **Step 3: Write minimal implementation if needed**
  Fix only CLI routing, output, or test-server wiring if the tests expose a mismatch.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p fantasma-cli --test http_flows projects_delete_uses_active_project -- --exact`
Run: `cargo test -p fantasma-cli --test http_flows project_deletions_list_uses_active_project -- --exact`
Run: `cargo test -p fantasma-cli --test http_flows project_deletions_get_uses_active_project -- --exact`

### Task 6: CLI range-delete request shape

**Files:**
- Modify: `crates/fantasma-cli/tests/http_flows.rs`

- [ ] **Step 1: Write the failing test**
  Add a CLI test proving `fantasma projects deletions range` sends the expected JSON body for `start_at`, `end_before`, `event`, repeated `--filter`, and repeated `--property`.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p fantasma-cli --test http_flows project_deletions_range_sends_expected_scope -- --exact`

- [ ] **Step 3: Write minimal implementation if needed**
  Fix only CLI serialization/parsing if the test exposes a mismatch.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p fantasma-cli --test http_flows project_deletions_range_sends_expected_scope -- --exact`

## Chunk 4: Final Verification And Project Memory

### Task 7: Verify the hardened slice

**Files:**
- Modify: `docs/STATUS.md`

- [ ] **Step 1: Run focused compile checks**

Run: `cargo test -p fantasma-store --lib --no-run`
Run: `cargo test -p fantasma-api --test auth --no-run`
Run: `cargo test -p fantasma-cli --test http_flows --no-run`
Run: `cargo test -p fantasma-worker --test pipeline --no-run`

- [ ] **Step 2: Run focused Docker-backed regressions**

Run: `FANTASMA_DOCKER_TEST_KEEP_CACHE=1 ./scripts/docker-test.sh -p fantasma-store project_processing_lease_detects_fence_epoch_advance -- --quiet`
Run: `FANTASMA_DOCKER_TEST_KEEP_CACHE=1 ./scripts/docker-test.sh -p fantasma-worker --test pipeline blocked_deletion_does_not_head_of_line_block_other_projects -- --quiet`
Run: `FANTASMA_DOCKER_TEST_KEEP_CACHE=1 ./scripts/docker-test.sh -p fantasma-worker --test pipeline stale_processing_leases_do_not_block_deletion_forever -- --quiet`

- [ ] **Step 3: Run workspace hygiene**

Run: `cargo fmt --all --check`
Run: `cargo clippy --workspace --all-targets -- -D warnings`

- [ ] **Step 4: Update project memory**
  Add one `docs/STATUS.md` bullet summarizing the additional test-hardening coverage and the exact commands that passed.
