# Security And Ingest Hardening Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove insecure operator-token defaults, add direct ingest crate coverage, and sanitize benchmark artifacts so the repository ships cleaner local security and privacy posture.

**Architecture:** Split the work into three bounded slices. First, make operator auth explicitly configured everywhere instead of silently falling back to a known dev token. Second, add crate-local ingest HTTP tests for the boundary behaviors currently covered only indirectly. Third, reduce benchmark publication metadata to a sanitized environment label and scrub existing committed artifacts to match.

**Tech Stack:** Rust, Axum, SQLx, Docker Compose, Bash, Markdown, JSON benchmark artifacts.

---

### Task 1: Harden Operator Token Configuration

**Files:**
- Modify: `crates/fantasma-api/src/main.rs`
- Modify: `crates/fantasma-auth/src/lib.rs`
- Modify: `infra/docker/compose.yaml`
- Modify: `infra/docker/compose.bench.yaml`
- Modify: `scripts/compose-smoke.sh`
- Modify: `scripts/cli-smoke.sh`
- Modify: `scripts/provision-project.sh`
- Modify: `docs/deployment.md`
- Test: `crates/fantasma-api/src/main.rs`

- [ ] **Step 1: Write the failing tests**

Add unit tests around the API binary env-loading path proving that operator auth fails closed when `FANTASMA_ADMIN_TOKEN` is missing and succeeds only when the env var is explicitly present.

- [ ] **Step 2: Run the focused test to verify red**

Run: `cargo test -p fantasma-api load_authorizer_from_env --bin fantasma-api`

Expected: FAIL because the env-loading helper does not yet exist or still falls back to the dev token.

- [ ] **Step 3: Implement the minimal hardening**

Require explicit `FANTASMA_ADMIN_TOKEN` in the API binary, remove the `StaticAdminAuthorizer` default impl that bakes in a dev token, require explicit token injection in checked-in Compose files, and make local smoke tooling generate/export an ephemeral token per run instead of relying on a fixed fallback. Keep `scripts/provision-project.sh` fail-closed unless the operator token is provided explicitly or via env.

- [ ] **Step 4: Run the focused test to verify green**

Run: `cargo test -p fantasma-api load_authorizer_from_env --bin fantasma-api`

Expected: PASS

### Task 2: Add Direct Ingest Crate Coverage

**Files:**
- Create: `crates/fantasma-ingest/tests/http.rs`
- Modify: `crates/fantasma-ingest/Cargo.toml`

- [ ] **Step 1: Write direct ingest HTTP tests**

Add crate-local tests for missing auth, malformed JSON, payload-too-large rejection, validation-envelope shape, and a happy-path accepted insert using the real router.

- [ ] **Step 2: Run the new ingest tests**

Run: `./scripts/docker-test.sh -p fantasma-ingest --test http`

Expected: PASS with direct ingest coverage instead of only indirect pipeline coverage.

### Task 3: Sanitize Benchmark Publication Metadata

**Files:**
- Modify: `crates/fantasma-bench/src/main.rs`
- Modify: `docs/performance.md`
- Modify: committed JSON artifacts under `artifacts/performance/`
- Modify: `crates/fantasma-bench/baselines/slo-iterative-default.json`

- [ ] **Step 1: Write the failing benchmark tests**

Add or update bench tests so summary rendering and artifact serialization expect sanitized environment metadata instead of detailed host fingerprints and exact benchmark timestamps.

- [ ] **Step 2: Run the focused bench test to verify red**

Run: `cargo test -p fantasma-bench host_metadata -- --nocapture`

Expected: FAIL because the harness still emits detailed host metadata.

- [ ] **Step 3: Implement sanitized metadata and scrub checked-in artifacts**

Change benchmark publication to write a coarse environment label only, update Markdown/JSON renderers and docs accordingly, and bulk-sanitize the committed artifacts so no checked-in benchmark outputs retain the old detailed host keys.

- [ ] **Step 4: Run the focused bench tests to verify green**

Run: `cargo test -p fantasma-bench --quiet`

Expected: PASS

### Task 4: Verify And Record Completion

**Files:**
- Modify: `docs/STATUS.md`

- [ ] **Step 1: Run the affected verification commands**

Run: `cargo fmt --all --check`

Expected: PASS

Run: `cargo clippy --workspace --all-targets -- -D warnings`

Expected: PASS

Run: `FANTASMA_ADMIN_TOKEN=test-token docker compose -f infra/docker/compose.yaml config -q`

Expected: PASS

Run: `FANTASMA_ADMIN_TOKEN=test-token docker compose -f infra/docker/compose.bench.yaml config -q`

Expected: PASS

Run: `./scripts/docker-test.sh -p fantasma-ingest --test http`

Expected: PASS

- [ ] **Step 2: Re-scan for stale risky material**

Run a repo-wide search to confirm no active crate/docs/scripts/infra code paths still default the operator token to the fixed dev value and no checked-in benchmark artifacts still publish detailed host fingerprint keys.

- [ ] **Step 3: Update project memory**

Record the hardening outcome, fresh verification, and any remaining follow-up in `docs/STATUS.md`.
