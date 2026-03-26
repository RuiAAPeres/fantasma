# C27 Phase 1 Through Phase 4 Stack Rebuild Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rebuild the intended benchmark stack from `c27dd24` by preserving the accepted Hotspot 3 ingest phase 1/2 slice already present in that commit, layering the proven session-lane phase 3/4 slice on top, and benchmarking that exact combined checkpoint.

**Architecture:** Keep `c27dd24` as the runtime base, preserve the accepted Hotspot 3 ingest path already present there, and layer only the reconstructed phase-3/4 session-lane changes from the working tree on top without pulling in later unrelated runtime experiments. Verify that the ingest slice still matches the accepted Hotspot 3 contract: one pre-body-read auth/state gate and one post-parse authenticated insert helper with the accepted lease timing. Then verify the combined worker/store/ingest stack and benchmark it directly against the accepted Hotspot 3 pair.

**Tech Stack:** Rust, Axum, Tokio, SQLx, Postgres, Docker-backed integration tests, `fantasma-ingest`, `fantasma-store`, `fantasma-worker`, `fantasma-bench`

---

## File Map

- Modify: `crates/fantasma-ingest/src/http.rs`
  Confirm the accepted Hotspot 3 wiring is still intact on the `c27` base and only touch it if source-shape or integration regressions require a repair.
- Modify: `crates/fantasma-ingest/tests/http.rs`
  Keep the Hotspot 3 HTTP regressions proving auth-before-body-read behavior and lifecycle gating.
- Modify: `crates/fantasma-store/src/lib.rs`
  Preserve the authenticated ingest context and helper surface already present in `c27`, while keeping the existing phase-3/4 store batch helpers already in the working tree.
- Modify: `crates/fantasma-worker/src/worker.rs`
  Keep the current phase-3/4 rebuild intact, only touching it if review or verification reveals an integration issue.
- Modify: `docs/STATUS.md`
  Record start, reviews, verification, benchmark evidence, and whether the combined stack is accepted.
- Create: `docs/superpowers/plans/2026-03-26-c27-phase1-phase4-stack-rebuild.md`
  Execution plan for this combined rebuild.
- Output: `artifacts/performance/2026-03-26-c27-phase1-phase4-stack-rebuild/`
  Representative append benchmark evidence for the exact combined checkpoint.

## Chunk 1: Lock The Combined Scope In Project Memory And Guard The Existing Hotspot 3 Base

### Task 1: Record the combined rebuild start and preserve the known-good session-lane evidence

**Files:**
- Modify: `docs/STATUS.md`

- [ ] **Step 1: Add a start note for the combined rebuild**

Record that the new slice is rebuilding the intended stacked checkpoint from `c27dd24`: preserve the Hotspot 3 ingest phase 1/2 base already in that commit, then layer session phase 3/4.

- [ ] **Step 2: Explicitly note what stays out of scope**

Capture that this rebuild excludes:
- bitmap enqueue batching
- session write-set fusion
- pool-scoped rebuild drain locks
- later event-lane whole-batch upsert experiments

### Task 2: Confirm the ingest phase 1/2 base is still present on `c27`

**Files:**
- Modify: `crates/fantasma-ingest/tests/http.rs`
- Modify: `crates/fantasma-store/src/lib.rs`

- [ ] **Step 1: Verify the accepted ingest HTTP regressions are already present**

Confirm focused coverage exists for:
- invalid key -> `401`
- revoked key -> `401`
- wrong-kind key -> `401`
- `range_deleting` project -> `409 project_busy`
- `pending_deletion` project -> `409 project_pending_deletion`
- body-read ordering proof for unauthorized and lifecycle-gated requests

- [ ] **Step 2: Verify the accepted store regressions are already present**

Confirm coverage exists for:
- `authenticate_ingest_request(...)`
- `insert_events_with_authenticated_ingest(...)`
- accepted lease timing that keeps the lease claim outside the raw insert transaction

- [ ] **Step 3: Run the focused regressions to prove the Hotspot 3 base is intact**

Run:
```bash
FANTASMA_DOCKER_TEST_KEEP_CACHE=1 ./scripts/docker-test.sh -p fantasma-ingest --test http --quiet
FANTASMA_DOCKER_TEST_KEEP_CACHE=1 ./scripts/docker-test.sh -p fantasma-store --lib --quiet
```

Expected:
- the accepted ingest regressions pass on the `c27` base before any session-lane benchmark work
## Chunk 2: Preserve The Phase 3/4 Rebuild And Review The Combined Runtime

### Task 3: Run subagent review on the combined checkpoint before benchmarking

**Files:**
- Modify: `crates/fantasma-store/src/lib.rs`
- Modify: `crates/fantasma-worker/src/worker.rs`
- Modify: `crates/fantasma-ingest/src/http.rs`

- [ ] **Step 1: Dispatch spec-compliance review**

Review against:
- `docs/superpowers/plans/2026-03-25-hotspot-3-ingest-fused-store-path.md`
- `docs/superpowers/plans/2026-03-25-phase3-rebuild-from-c27.md`
- this combined rebuild plan

- [ ] **Step 2: Dispatch code-quality review**

Focus on:
- ingest auth-before-body-read behavior
- lease/fence correctness
- worker append ordering and replay semantics
- batch writer bind-limit safety

- [ ] **Step 3: Address any findings before benchmark acceptance**

### Task 4: Run full verification and benchmark the combined stack

**Files:**
- Modify: `docs/STATUS.md`

- [ ] **Step 1: Run the required verification**

Run:
```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
FANTASMA_DOCKER_TEST_KEEP_CACHE=1 ./scripts/docker-test.sh -p fantasma-store --lib --quiet
FANTASMA_DOCKER_TEST_KEEP_CACHE=1 ./scripts/docker-test.sh -p fantasma-ingest --test http --quiet
FANTASMA_DOCKER_TEST_KEEP_CACHE=1 ./scripts/docker-test.sh -p fantasma-worker --lib --quiet
FANTASMA_DOCKER_TEST_KEEP_CACHE=1 ./scripts/docker-test.sh -p fantasma-worker --test pipeline --quiet
git diff --check
```

Expected:
- all commands exit 0

- [ ] **Step 2: Run the representative append benchmark pair**

Run:
```bash
FANTASMA_ADMIN_TOKEN=test-token cargo run -p fantasma-bench -- slo --output-dir artifacts/performance/2026-03-26-c27-phase1-phase4-stack-rebuild --scenario live-append-small-blobs --scenario live-append-plus-light-repair
```

Expected:
- compare against accepted Hotspot 3 `130623ms` and `122770ms`
- preserve or improve the current phase-3/4-only result if possible

- [ ] **Step 3: Record the result in `docs/STATUS.md`**

Capture:
- verification commands run
- benchmark deltas versus Hotspot 3
- whether the combined phase-1/2/3/4 stack is accepted or backed out
