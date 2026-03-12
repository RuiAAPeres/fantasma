# Deployment

## Local Development Target

Fantasma should be runnable locally with:

```bash
docker compose -f infra/docker/compose.yaml up --build
```

The current vertical slice Compose setup includes:

- Postgres
- `fantasma-ingest`
- `fantasma-api`
- `fantasma-worker`
- optional `dashboard`

Current repository files:

- Compose file: `infra/docker/compose.yaml`
- Docker test Compose file: `infra/docker/compose.test.yaml`
- Service images: `infra/docker/Dockerfile.ingest`, `infra/docker/Dockerfile.api`, `infra/docker/Dockerfile.worker`
- Docker test image: `infra/docker/Dockerfile.test`

## Deployment Principles

- Prefer a small number of services.
- Avoid optional infrastructure in v1 unless required by a concrete bottleneck.
- Make it easy to run locally and in a simple self-hosted environment.
- Keep configuration explicit through environment variables.

Operational guidance:

- Fantasma prefers simple, explicit, high-performance data paths.
- We avoid steady-state designs that rely on broad recomputation.
- We narrow feature shape when necessary to preserve predictable performance and operational clarity.
- Correctness still matters, but we should choose correctness models that compose with incremental processing.
- Backend sessionization is internal; clients send event context, not session identity.
- Event `properties` are for product context and must not contain direct identifiers or sensitive personal data.
- Event `properties` are explicit string-to-string context, capped at 3 keys, with canonical keys matching `^[a-z][a-z0-9_]{0,62}$`.
- The 3-key cap is a deliberate performance boundary: event metrics are backed by bounded worker-built aggregates, so keeping event context narrow preserves predictable write amplification, storage growth, and query behavior.
- First-party supported SDKs auto-populate `platform`, `app_version`, and `os_version`.
- DB-backed Rust tests are expected to run fully in Docker through the repository workflow.
- Keep workspace tests on the Docker Postgres path limited to cases satisfied by Postgres alone; stack-dependent checks stay in dedicated smoke workflows.

## Planned Environment Variables

Shared examples:

- `FANTASMA_DATABASE_URL`
- `FANTASMA_BIND_ADDRESS`
- `FANTASMA_LOG_LEVEL`
- `FANTASMA_PROJECT_ID`
- `FANTASMA_PROJECT_NAME`

Service-specific examples:

- `FANTASMA_INGEST_KEY`
- `FANTASMA_ADMIN_TOKEN`
- `FANTASMA_WORKER_POLL_INTERVAL_MS`
- `FANTASMA_WORKER_BATCH_SIZE`

## Startup Schema Preparation

On startup, `fantasma-ingest`, `fantasma-api`, and `fantasma-worker` all:

- connect to Postgres using a pooled connection
- run SQL migrations from `crates/fantasma-store/migrations/`
- optionally seed the local development project plus ingest key when seed environment variables are present

The schema is owned by `fantasma-store` through `sqlx::migrate!()`, while local project seeding stays explicit.

## Local Preconditions

Reliable local verification currently requires:

- enough free disk for Cargo, Docker layers, and Postgres data
- a healthy Docker daemon

The current machine state that prompted this work had both Docker I/O failures and very low free space. Use the smoke script below as the first preflight check.

## Local Docker Workspace Tests

Run DB-backed Rust tests fully inside Docker:

```bash
./scripts/docker-test.sh
```

The Docker test workflow uses:

- `infra/docker/compose.test.yaml`
- a disposable Postgres container with health checks
- `DATABASE_URL=postgres://fantasma:fantasma@postgres:5432/postgres`
- the `fantasma` role as the `sqlx::test` bootstrap user, which must retain `CREATEDB`

The Docker test runner mounts the current checkout and keeps Cargo caches outside the repository tree. By default it tears down containers on exit with `docker compose down --remove-orphans`. Set `FANTASMA_DOCKER_TEST_KEEP_CONTAINERS=1` to keep containers around for debugging after a failure.

Test taxonomy:

- `./scripts/docker-test.sh` is the canonical path for Rust workspace tests that need Postgres
- `./scripts/compose-smoke.sh` is for running-stack verification and should stay separate from workspace `cargo test`

## Performance Verification

Fantasma keeps numeric stack benchmarks separate from the normal smoke path:

- benchmark Compose file: [`infra/docker/compose.bench.yaml`](../infra/docker/compose.bench.yaml)
- benchmark docs and commands: [`docs/performance.md`](performance.md)

The benchmark stack keeps the same services as local development, but it runs under its own Compose project (`fantasma-bench`), uses benchmark-only host ports (`18081` / `18082`), does not publish Postgres on the host, and lowers the worker poll interval so derive-lag measurements are meaningful instead of dominated by the default 5-second sleep.

## Local Smoke Test

Run the preflighted smoke script:

```bash
./scripts/compose-smoke.sh
```

The smoke script uses a per-run disposable Compose project name by default, waits for the ingest and API `/health` endpoints before sending events, and validates the supported worker-derived session and event metrics. It still binds the standard local ports on `localhost`. Set `FANTASMA_SMOKE_PROJECT_NAME` if you need a stable override for debugging.

Manual flow:

```bash
docker compose -f infra/docker/compose.yaml up --build
```

Send one event:

```bash
curl -X POST http://localhost:8081/v1/events \
  -H "Content-Type: application/json" \
  -H "X-Fantasma-Key: fg_ing_test" \
  -d '{
    "events": [
      {
        "event": "app_open",
        "timestamp": "2026-01-01T00:00:00Z",
        "install_id": "abc",
        "platform": "ios",
        "app_version": "1.0",
        "os_version": "18.3",
        "properties": {
          "provider": "strava"
        }
      }
    ]
  }'
```

Query the worker-derived event aggregate after the worker has processed the new raw events:

```bash
curl "http://localhost:8082/v1/metrics/events/aggregate?project_id=9bad8b88-5e7a-44ed-98ce-4cf9ddde713a&event=app_open&start_date=2026-01-01&end_date=2026-01-02&platform=ios&group_by=provider" \
  -H "Authorization: Bearer fg_pat_dev"
```

Query the worker-derived daily event series:

```bash
curl "http://localhost:8082/v1/metrics/events/daily?project_id=9bad8b88-5e7a-44ed-98ce-4cf9ddde713a&event=app_open&start_date=2026-01-01&end_date=2026-01-02&group_by=provider" \
  -H "Authorization: Bearer fg_pat_dev"
```

Query the derived session metrics after the worker has processed the new raw events:

```bash
curl "http://localhost:8082/v1/metrics/sessions/count?project_id=9bad8b88-5e7a-44ed-98ce-4cf9ddde713a&start=2026-01-01T00:00:00Z&end=2026-01-02T00:00:00Z" \
  -H "Authorization: Bearer fg_pat_dev"
```

```bash
curl "http://localhost:8082/v1/metrics/sessions/duration?project_id=9bad8b88-5e7a-44ed-98ce-4cf9ddde713a&start=2026-01-01T00:00:00Z&end=2026-01-02T00:00:00Z" \
  -H "Authorization: Bearer fg_pat_dev"
```

```bash
curl "http://localhost:8082/v1/metrics/active-installs?project_id=9bad8b88-5e7a-44ed-98ce-4cf9ddde713a&start=2026-01-01T00:00:00Z&end=2026-01-02T00:00:00Z" \
  -H "Authorization: Bearer fg_pat_dev"
```

Poll the daily endpoint until the expected data appears or the timeout expires:

```bash
curl "http://localhost:8082/v1/metrics/sessions/count/daily?project_id=9bad8b88-5e7a-44ed-98ce-4cf9ddde713a&start_date=2026-01-01&end_date=2026-01-02" \
  -H "Authorization: Bearer fg_pat_dev"
```

The daily routes use inclusive UTC date bounds:

- `/v1/metrics/events/daily`
- `/v1/metrics/sessions/count/daily`
- `/v1/metrics/sessions/duration/total/daily`

Event-metrics query rules:

- built-in filters use normal query params: `platform`, `app_version`, `os_version`
- any other non-reserved query key becomes an equality filter on an explicit string property
- `group_by` may be repeated up to twice
- event metrics are eventually consistent because reads come from worker-built daily rollups

## iOS Demo App

Start the local backend first:

```bash
docker compose -f infra/docker/compose.yaml up --build
```

Then open the demo app in Xcode:

```bash
open apps/demo-ios/FantasmaDemo.xcodeproj
```

Run the app in the iOS Simulator. The demo configures the SDK for `http://localhost:8081` with `fg_ing_test`, sends `app_open`, sends `screen_view` for the home screen, and lets you enqueue `button_pressed` events from the main button.

The resulting metrics are install-based. If the same human uses the app on two devices, Fantasma counts those as two installs by design.

Use the worker-derived event and session endpoints to confirm the full pipeline after interacting with the app:

```bash
curl "http://localhost:8082/v1/metrics/events/aggregate?project_id=9bad8b88-5e7a-44ed-98ce-4cf9ddde713a&event=app_open&start_date=2026-01-01&end_date=2027-01-01&group_by=provider" \
  -H "Authorization: Bearer fg_pat_dev"
```

```bash
curl "http://localhost:8082/v1/metrics/sessions/count?project_id=9bad8b88-5e7a-44ed-98ce-4cf9ddde713a&start=2026-01-01T00:00:00Z&end=2027-01-01T00:00:00Z" \
  -H "Authorization: Bearer fg_pat_dev"
```
