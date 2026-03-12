# Docker Test Workflow Design

## Goal

Make Fantasma's DB-backed Rust tests runnable fully inside Docker so local verification does not depend on host Postgres or ad hoc `DATABASE_URL` setup.

## Repo Preference

Fantasma's standing preference is:

- DB-backed Rust tests should run fully in Docker.
- Local verification should not require a host-installed Postgres instance.
- Local verification should not require manual `DATABASE_URL` setup outside the provided Docker test workflow.

This preference should be written into repository guidance during implementation so it remains consistent going forward.

## Current State

- CI already runs Rust tests with a containerized Postgres service and `DATABASE_URL`.
- Local Docker support is currently shaped around the application stack in `infra/docker/compose.yaml`.
- DB-backed `#[sqlx::test]` tests fail locally unless `DATABASE_URL` is set in the host shell.
- `scripts/compose-smoke.sh` verifies the running stack, but it is not a workspace test runner.

## Desired Outcome

Fantasma should have a first-class Docker test workflow that:

- starts a disposable Postgres container with health checks
- runs `cargo test --workspace` inside a Rust toolchain container
- injects the correct `DATABASE_URL` from Docker networking
- works from the current checkout without requiring host Rust or host Postgres
- fails clearly and surfaces logs when the test database never becomes healthy

## Recommended Approach

Use a dedicated Compose-based test workflow that is separate from the runtime app stack.

### Why this approach

- It matches the existing CI mental model closely.
- It keeps app-runtime verification and workspace-test verification as separate concerns.
- It avoids coupling workspace tests to ingest/api/worker service startup.
- It keeps the operational contract simple: one Docker entrypoint for DB-backed tests.

## Scope

### In scope

- add a Docker Compose test definition or profile for workspace tests
- add a dedicated Rust test-runner image or service
- add a repo script that runs workspace tests fully inside Docker
- update docs and repo guidance to prefer Docker for DB-backed test execution
- keep the existing smoke script for stack-level behavior

### Out of scope

- replacing the runtime Compose stack
- adding non-Postgres infrastructure
- changing the application architecture
- introducing a custom test harness outside normal `cargo test`

## Proposed Design

### 1. Separate runtime and test Compose concerns

Keep `infra/docker/compose.yaml` focused on the app stack. Add a dedicated Docker test definition, for example:

- `infra/docker/compose.test.yaml`

This file should define:

- `postgres`
- `workspace-tests`

The `workspace-tests` service should:

- build from the repo root
- mount the working tree
- run from the mounted working tree
- receive `DATABASE_URL=postgres://fantasma:fantasma@postgres:5432/fantasma`
- depend on Postgres health rather than just container startup

### 2. Add a Rust workspace test-runner image

Add a Dockerfile dedicated to test execution, for example:

- `infra/docker/Dockerfile.test`

It should contain only what is necessary to run workspace tests:

- Rust toolchain
- system dependencies required by the workspace
- a sensible working directory

The image should not embed the source tree permanently; Compose should mount the current checkout so the test container runs current branch state.

### 3. Add one entrypoint script for local use

Add a script, for example:

- `scripts/docker-test.sh`

Responsibilities:

- check Docker daemon availability
- bring up the test Postgres service
- wait for health
- run `cargo test --workspace` inside `workspace-tests`
- return the test exit code
- print relevant container logs on failure
- tear down containers by default

Optional flags can be added later, but v1 should optimize for one obvious happy path.

### 4. Keep smoke verification separate

Do not merge `scripts/compose-smoke.sh` with the new Docker test workflow.

The separation should remain:

- `scripts/docker-test.sh` for workspace correctness and DB-backed tests
- `scripts/compose-smoke.sh` for stack behavior after services are running

This keeps failure modes easier to interpret.

### 5. Align local docs and repo memory

Update the written repo guidance so the preference is explicit in:

- `AGENTS.md`
- `docs/deployment.md`
- `docs/STATUS.md`

The rule should say that DB-backed Rust tests are expected to run fully in Docker unless the repo later makes a deliberate change.

## Data Flow

```text
developer
  -> scripts/docker-test.sh
  -> docker compose -f infra/docker/compose.test.yaml up
  -> postgres healthcheck passes
  -> workspace-tests container runs cargo test --workspace
  -> sqlx::test reads DATABASE_URL from container env
  -> tests create/use isolated databases against Docker Postgres
```

## Error Handling

- If Docker is unavailable, fail before starting services.
- If Postgres never becomes healthy, print container status and logs, then fail.
- If `cargo test --workspace` fails, return the test exit code and keep the error output intact.
- If teardown fails, report it without hiding the real test result.

## Testing Strategy

### Test the workflow itself

- validate the new Compose file with `docker compose ... config`
- shell-check or `bash -n` the new script
- run the script in a healthy environment and confirm DB-backed tests start without host `DATABASE_URL`

### Keep existing Rust verification

- continue using `cargo test --workspace --no-run` for fast compile verification
- use the new Docker path for real DB-backed runtime execution

## Migration Notes

- CI may keep using GitHub Actions service containers for now if that stays simpler.
- The local Docker workflow should still mirror CI assumptions:
  - containerized Postgres
  - explicit `DATABASE_URL`
  - one command to run workspace tests

## Success Criteria

- A developer can run one documented command and execute DB-backed workspace tests fully inside Docker.
- `sqlx::test` no longer depends on host shell `DATABASE_URL` for the supported local path.
- Repo docs explicitly record the Docker-only preference for DB-backed tests.
- Smoke verification remains separate from workspace test execution.
