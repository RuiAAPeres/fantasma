# TimescaleDB End-to-End Benchmark Spike Implementation Plan

> Outcome: retained as reference only. The March 17, 2026 branch-local spike was discarded after benchmarking, and Fantasma stayed on Postgres.

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a benchmark-only Timescale backend that can run the same end-to-end `reads-visibility-30d` scenario as the Postgres control while keeping the public API unchanged.

**Architecture:** Keep the control path on stock Postgres and turn the Timescale path into a real alternate backend, not a legality probe. The Timescale benchmark backend may redefine raw-event identity, indexes, and worker progress semantics as needed, but it must preserve the same benchmark scenario shape, API routes, readiness metrics, and result artifact layout.

**Tech Stack:** Rust, sqlx, Docker Compose, Postgres 17, TimescaleDB 2.25.2

---

## Chunk 1: Reframe The Spike Around Comparable End-to-End Runs

### Task 1: Update project memory and operator-facing docs

**Files:**
- Modify: `docs/STATUS.md`
- Modify: `docs/performance.md`
- Modify: `docs/deployment.md`

- [ ] **Step 1: Rewrite the active Timescale status entry**
  State that the spike is now a benchmark-only alternate backend design, not an `events_raw` legality stop-check.

- [ ] **Step 2: Update benchmark docs to describe the new success bar**
  Document that the Timescale backend must complete the same `reads-visibility-30d` run as Postgres to produce comparable numbers.

- [ ] **Step 3: Update deployment docs for the benchmark-only branch semantics**
  Keep the default stack untouched and describe the Timescale compose file as a benchmark-only alternate backend.

## Chunk 2: Make The Timescale Raw-Event Store Native Instead Of Constrained

### Task 2: Add failing tests for backend-specific raw-event progress assumptions

**Files:**
- Modify: `crates/fantasma-bench/src/main.rs`
- Test: `crates/fantasma-bench/src/main.rs`

- [ ] **Step 1: Add a failing test that Timescale benchmark preparation does not stop at the legacy hypertable legality gate**
- [ ] **Step 2: Add a failing test that benchmark metadata can distinguish a fully runnable Timescale backend from a feasibility-only failure**
- [ ] **Step 3: Run the targeted bench test filter and verify the new tests fail**
  Run: `cargo test -p fantasma-bench timescale -- --nocapture`
  Expected: FAIL because the current backend still attempts a transparent hypertable swap and exits early.

### Task 3: Replace the Timescale benchmark schema/bootstrap with a backend-native raw table design

**Files:**
- Modify: `crates/fantasma-bench/src/main.rs`
- Modify: `infra/docker/compose.bench.timescale.yaml`
- Modify: `infra/docker/timescaledb-init/001-enable-extension.sql`

- [ ] **Step 1: Introduce a benchmark-only Timescale schema setup path**
  Add explicit backend preparation code that can reshape `events_raw` for Timescale instead of trying to preserve the stock primary key.

- [ ] **Step 2: Define a Timescale-native `events_raw` identity/index contract**
  Keep deterministic ordering for worker use, but make the hypertable legal with partition-column-aware uniqueness.

- [ ] **Step 3: Keep benchmark metadata/reporting intact**
  Record the Timescale version, chunk interval, and whether the backend completed the alternate schema preparation successfully.

- [ ] **Step 4: Run the targeted bench test filter and make the new tests pass**
  Run: `cargo test -p fantasma-bench timescale -- --nocapture`
  Expected: PASS for the new backend-preparation tests.

## Chunk 3: Make Worker Progress Comparable On The Timescale Backend

### Task 4: Add failing tests for backend-specific worker event ordering/progress

**Files:**
- Modify: `crates/fantasma-worker/src/worker.rs`
- Modify: `crates/fantasma-store/src/lib.rs`
- Test: `crates/fantasma-worker/src/worker.rs`

- [ ] **Step 1: Add a failing test that a Timescale backend can fetch a deterministic raw-event batch without relying on global `id` ordering alone**
- [ ] **Step 2: Add a failing test that the benchmark worker progress path can advance on the Timescale backend using its backend-specific cursor semantics**
- [ ] **Step 3: Run the targeted worker test filter and verify the new tests fail**
  Run: `cargo test -p fantasma-worker timescale -- --nocapture`
  Expected: FAIL because the worker/store path is still hard-wired to `last_processed_event_id`.

### Task 5: Implement a Timescale-only benchmark worker cursor/progress path

**Files:**
- Modify: `crates/fantasma-worker/src/worker.rs`
- Modify: `crates/fantasma-store/src/lib.rs`

- [ ] **Step 1: Introduce a benchmark-backend-aware raw-event fetch contract**
  Let the Timescale backend fetch raw events using a deterministic composite ordering compatible with the new hypertable schema.

- [ ] **Step 2: Introduce backend-specific worker progress storage/serialization**
  Keep the stock Postgres path unchanged while allowing the Timescale backend to checkpoint with the new cursor shape.

- [ ] **Step 3: Adapt repair-path fetches and stale-frontier checks**
  Make sure append and repair both use the same ordering contract on the Timescale backend.

- [ ] **Step 4: Run the targeted worker tests and make them pass**
  Run: `cargo test -p fantasma-worker timescale -- --nocapture`
  Expected: PASS for the new worker/store tests.

## Chunk 4: Preserve Comparable Public Results And Query Evidence

### Task 6: Add failing tests for parity and benchmark-side validation

**Files:**
- Modify: `crates/fantasma-bench/src/main.rs`
- Test: `crates/fantasma-bench/src/main.rs`

- [ ] **Step 1: Add a failing test that raw-discovery/read artifacts remain structurally identical across backends**
- [ ] **Step 2: Add a failing test that the Timescale benchmark path must validate benchmarked responses before timing them**
- [ ] **Step 3: Run the targeted bench test filter and verify the new tests fail**
  Run: `cargo test -p fantasma-bench raw_discovery -- --nocapture`
  Expected: FAIL if the alternate backend path bypasses the existing validation/artifact contract.

### Task 7: Keep the benchmark query/artifact boundary identical

**Files:**
- Modify: `crates/fantasma-bench/src/main.rs`

- [ ] **Step 1: Keep the same `reads-visibility-30d` query matrix and output files for both backends**
- [ ] **Step 2: Keep response validation active for the Timescale path**
- [ ] **Step 3: Keep `raw-discovery-explain.json` generation working for the Timescale path**
- [ ] **Step 4: Re-run the targeted bench tests**
  Run: `cargo test -p fantasma-bench --quiet`
  Expected: PASS

### Task 8: Add explicit cross-backend contract and parity checks

**Files:**
- Modify: `crates/fantasma-bench/src/main.rs`

- [ ] **Step 1: Add a failing test for backend-parity verification helpers**
  The helper should compare benchmark-visible readiness fields and route-level payload totals for the same seeded scenario output.

- [ ] **Step 2: Add a benchmark contract check before timing publication**
  Verify the same seeded scenario shape, benchmarked routes, and public response semantics are exercised on both backends.

- [ ] **Step 3: Add a post-run parity check for benchmark-visible outputs**
  Compare the benchmark-visible result payloads needed for apples-to-apples interpretation before treating the Timescale run as comparable.

- [ ] **Step 4: Re-run targeted bench tests**
  Run: `cargo test -p fantasma-bench parity -- --nocapture`
  Expected: PASS

## Chunk 5: Run Comparable Benchmarks And Publish The Spike Result

### Task 9: Verify both backends can produce comparable `reads-visibility-30d` runs

**Files:**
- Modify: `docs/STATUS.md`

- [ ] **Step 1: Run bench/unit verification**
  Run: `cargo test -p fantasma-bench --quiet`

- [ ] **Step 2: Run worker verification**
  Run: `cargo test -p fantasma-worker timescale -- --nocapture`

- [ ] **Step 3: Run formatting and lint checks**
  Run: `cargo fmt --all --check`
  Run: `cargo clippy --workspace --all-targets -- -D warnings`

- [ ] **Step 4: Validate both benchmark compose stacks**
  Run: `FANTASMA_ADMIN_TOKEN=test-token docker compose -f infra/docker/compose.bench.yaml config`
  Run: `FANTASMA_ADMIN_TOKEN=test-token docker compose -f infra/docker/compose.bench.timescale.yaml config`

- [ ] **Step 5: Run the Postgres control benchmark**
  Run: `FANTASMA_ADMIN_TOKEN=test-token cargo run -p fantasma-bench -- slo --output-dir artifacts/performance/2026-03-17-timescaledb-spike-postgres --scenario reads-visibility-30d --database-backend postgres`

- [ ] **Step 6: Run the Timescale benchmark**
  Run: `FANTASMA_ADMIN_TOKEN=test-token cargo run -p fantasma-bench -- slo --output-dir artifacts/performance/2026-03-17-timescaledb-spike-timescale --scenario reads-visibility-30d --database-backend timescale`

- [ ] **Step 7: Update `docs/STATUS.md` with the final comparison**
  Record whether the Timescale backend completed the run, the ingest/readiness/query numbers, and any benchmark-only backend design notes needed to interpret the result.
