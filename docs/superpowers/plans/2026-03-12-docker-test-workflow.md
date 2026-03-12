# Docker Test Workflow Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Fantasma's DB-backed Rust tests runnable fully inside Docker with a first-class local command.

**Architecture:** Add a dedicated Docker Compose test workflow with its own Postgres service and Rust test-runner container, then route local DB-backed verification through a script that runs `cargo test --workspace` inside Docker. Keep runtime smoke verification separate from workspace test execution.

**Tech Stack:** Docker Compose, Bash, Rust workspace, SQLx/Postgres, existing Cargo test workflow.

---

## Chunk 1: Docker Test Runner Definition

### Task 1: Add the failing workflow contract to docs and scripts

**Files:**
- Modify: `docs/STATUS.md`
- Modify: `docs/deployment.md`
- Modify: `AGENTS.md`
- Test: `scripts/docker-test.sh`

- [ ] Step 1: Document the new repo preference before implementation
- [ ] Step 2: State explicitly that DB-backed Rust tests are expected to run fully in Docker
- [ ] Step 3: Add a placeholder `scripts/docker-test.sh` contract or TODO note if helpful for red-green planning
- [ ] Step 4: Verify docs still read coherently

### Task 2: Add dedicated Docker test definitions

**Files:**
- Create: `infra/docker/Dockerfile.test`
- Create: `infra/docker/compose.test.yaml`

- [ ] Step 1: Write the minimal test-runner Dockerfile
- [ ] Step 2: Add a test Compose file with:
  - `postgres`
  - `workspace-tests`
- [ ] Step 3: Configure `DATABASE_URL` to point at Docker Postgres
- [ ] Step 4: Add Postgres health checks and service dependency conditions
- [ ] Step 5: Run `docker compose -f infra/docker/compose.test.yaml config`
- [ ] Step 6: Fix any config errors until Compose validation passes

## Chunk 2: Local Test Entrypoint

### Task 3: Add the local Docker test script with TDD

**Files:**
- Create: `scripts/docker-test.sh`
- Modify: `docs/deployment.md`

- [ ] Step 1: Write down the expected script behavior in comments or docs:
  - Docker preflight
  - start test Postgres
  - wait for health
  - run `cargo test --workspace` inside Docker
  - print logs on failure
  - tear down by default
- [ ] Step 2: Implement the minimal script
- [ ] Step 3: Run `bash -n scripts/docker-test.sh`
- [ ] Step 4: Run `docker compose -f infra/docker/compose.test.yaml config`
- [ ] Step 5: Adjust the script until the static checks pass

### Task 4: Make the script ergonomic and predictable

**Files:**
- Modify: `scripts/docker-test.sh`
- Modify: `docs/deployment.md`

- [ ] Step 1: Ensure the script uses the current checkout, not a baked-in source snapshot
- [ ] Step 2: Ensure failure output preserves the real Cargo exit status
- [ ] Step 3: Ensure teardown behavior is explicit and understandable
- [ ] Step 4: Document the one canonical command for local Docker-backed Rust tests

## Chunk 3: Verification and Repo Consistency

### Task 5: Verify the Docker path and keep smoke path separate

**Files:**
- Modify: `scripts/compose-smoke.sh` only if documentation references need adjustment
- Modify: `docs/STATUS.md`

- [ ] Step 1: Confirm `scripts/compose-smoke.sh` remains stack verification, not workspace test execution
- [ ] Step 2: Run the new Docker test workflow in a healthy environment
- [ ] Step 3: Record the actual result in `docs/STATUS.md`
- [ ] Step 4: If the environment still has Docker I/O or disk issues, record that explicitly instead of claiming success

### Task 6: Final verification

**Files:**
- Modify as needed based on verification fixes

- [ ] Step 1: Run `bash -n scripts/docker-test.sh`
- [ ] Step 2: Run `docker compose -f infra/docker/compose.test.yaml config`
- [ ] Step 3: Run the Docker-backed test command end to end
- [ ] Step 4: Run `cargo fmt --all` if any source files changed
- [ ] Step 5: Summarize what passed and any remaining environment blockers
