# Deployment

Fantasma is documented for a single-host Docker Compose deployment. The
repository's primary deployment definition lives in
`infra/docker/compose.yaml`.

## Supported Topology

The default stack includes:

- Postgres
- `fantasma-ingest`
- `fantasma-api`
- `fantasma-worker`
- optional `dashboard` under the Compose `dashboard` profile

Repository files:

- Compose stack: `infra/docker/compose.yaml`
- service images: `infra/docker/Dockerfile.ingest`,
  `infra/docker/Dockerfile.api`, `infra/docker/Dockerfile.worker`
- benchmark stack: [`infra/docker/compose.bench.yaml`](../infra/docker/compose.bench.yaml)

## Configuration

Shared runtime variables:

- `FANTASMA_DATABASE_URL`: required by ingest, API, and worker
- `FANTASMA_LOG_LEVEL`: optional tracing filter
- `FANTASMA_PROJECT_ID`: optional local bootstrap override; binaries fall
  back to the repository's default local project UUID when unset
- `FANTASMA_PROJECT_NAME`: optional local bootstrap override; binaries fall
  back to `Local Development Project`
- `FANTASMA_INGEST_KEY`: optional local bootstrap ingest key

HTTP service variables:

- `FANTASMA_BIND_ADDRESS`: bind address for `fantasma-ingest` and
  `fantasma-api`
- `FANTASMA_ADMIN_TOKEN`: bearer token consumed by `fantasma-api`; defaults
  to `fg_pat_dev`

Worker variables:

- `FANTASMA_WORKER_POLL_INTERVAL_MS`: worker sleep interval in milliseconds;
  defaults to `5000`
- `FANTASMA_WORKER_BATCH_SIZE`: worker batch size; defaults to `500`

Current Compose defaults:

- all three services get
  `postgres://fantasma:fantasma@postgres:5432/fantasma`
- `fantasma-ingest` gets `0.0.0.0:8081`, `fg_ing_test`,
  `fg_pat_dev`, and explicit local project id/name values
- `fantasma-api` gets `0.0.0.0:8082`, `fg_ing_test`,
  `fg_pat_dev`, and explicit local project id/name values
- `fantasma-worker` gets `FANTASMA_WORKER_POLL_INTERVAL_MS=5000` and uses
  its built-in local project defaults because the Compose file does not pass
  project id/name or ingest key to the worker

## Startup Behavior

On startup, `fantasma-ingest`, `fantasma-api`, and `fantasma-worker` all:

- connect to Postgres using a pooled connection
- run SQL migrations from `crates/fantasma-store/migrations/`
- ensure the seeded local project exists using the configured project id,
  project name, and any provided ingest key

There is no separate migration job in the default stack. The dashboard, when
enabled, serves static files from `apps/dashboard-web/public` through Nginx.

## Preconditions

Before bringing the stack up, confirm:

- enough free disk for Docker layers, Cargo build output, and Postgres data
- a healthy Docker daemon
- `docker` and `curl` are installed for the smoke path

## Bring Up

Start the default stack:

```bash
docker compose -f infra/docker/compose.yaml up --build
```

Enable the dashboard profile:

```bash
docker compose -f infra/docker/compose.yaml --profile dashboard up --build
```

Default host ports:

- `5432`: Postgres
- `8081`: ingest
- `8082`: API
- `8080`: dashboard when enabled

Stop the stack and remove volumes:

```bash
docker compose -f infra/docker/compose.yaml down --volumes --remove-orphans
```

## Smoke Verification

Run the preflighted smoke script:

```bash
./scripts/compose-smoke.sh
```

The smoke path:

- checks free disk and Docker availability
- runs the stack in a disposable Compose project
- waits for `http://localhost:8081/health` and
  `http://localhost:8082/health`
- ingests sample events
- polls worker-derived metrics until the expected data appears
- tears the stack down with volumes on exit

Set `FANTASMA_SMOKE_PROJECT_NAME` if you need a stable Compose project name
for debugging.

Worker-built metrics are asynchronous. If you verify manually, expect a short
delay between ingesting events and seeing derived metric reads update.

## Manual Checks

Use these commands when you need to inspect the running stack without the smoke
script.

Health endpoints:

```bash
curl -fsS http://localhost:8081/health
curl -fsS http://localhost:8082/health
```

Ingest sample events:

```bash
curl -fsS -X POST http://localhost:8081/v1/events \
  -H "Content-Type: application/json" \
  -H "X-Fantasma-Key: fg_ing_test" \
  -d '{
    "events": [
      {
        "event": "app_open",
        "timestamp": "2026-01-01T00:00:00Z",
        "install_id": "compose-install-1",
        "platform": "ios",
        "app_version": "1.0.0",
        "os_version": "18.3",
        "properties": {
          "provider": "strava"
        }
      },
      {
        "event": "app_open",
        "timestamp": "2026-01-01T00:10:00Z",
        "install_id": "compose-install-1",
        "platform": "ios",
        "app_version": "1.0.0",
        "os_version": "18.3",
        "properties": {
          "provider": "strava"
        }
      }
    ]
  }'
```

Query derived metrics:

```bash
curl -fsS "http://localhost:8082/v1/metrics/sessions/count/daily?project_id=9bad8b88-5e7a-44ed-98ce-4cf9ddde713a&start_date=2026-01-01&end_date=2026-01-02" \
  -H "Authorization: Bearer fg_pat_dev"
```

```bash
curl -fsS "http://localhost:8082/v1/metrics/sessions/duration/total/daily?project_id=9bad8b88-5e7a-44ed-98ce-4cf9ddde713a&start_date=2026-01-01&end_date=2026-01-02" \
  -H "Authorization: Bearer fg_pat_dev"
```

```bash
curl -fsS "http://localhost:8082/v1/metrics/events/aggregate?project_id=9bad8b88-5e7a-44ed-98ce-4cf9ddde713a&event=app_open&start_date=2026-01-01&end_date=2026-01-02&platform=ios&group_by=provider" \
  -H "Authorization: Bearer fg_pat_dev"
```

```bash
curl -fsS "http://localhost:8082/v1/metrics/events/daily?project_id=9bad8b88-5e7a-44ed-98ce-4cf9ddde713a&event=app_open&start_date=2026-01-01&end_date=2026-01-02&group_by=provider" \
  -H "Authorization: Bearer fg_pat_dev"
```

## Related Docs

- architecture and data flow: [`docs/architecture.md`](architecture.md)
- benchmark-specific commands: [`docs/performance.md`](performance.md)
- iOS demo walkthrough: [`apps/demo-ios/README.md`](../apps/demo-ios/README.md)
