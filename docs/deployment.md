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
- service images: `infra/docker/Dockerfile.ingest`, `infra/docker/Dockerfile.api`, `infra/docker/Dockerfile.worker`
- benchmark stack: [`infra/docker/compose.bench.yaml`](../infra/docker/compose.bench.yaml)
- local provisioning helper: [`scripts/provision-project.sh`](../scripts/provision-project.sh)

## Configuration

Shared runtime variables:

- `FANTASMA_DATABASE_URL`: required by ingest, API, and worker
- `FANTASMA_LOG_LEVEL`: optional tracing filter

HTTP service variables:

- `FANTASMA_BIND_ADDRESS`: bind address for `fantasma-ingest` and `fantasma-api`
- `FANTASMA_ADMIN_TOKEN`: operator bearer token consumed only by `fantasma-api`; defaults to `fg_pat_dev`

Worker variables:

- `FANTASMA_WORKER_POLL_INTERVAL_MS`: worker sleep interval in milliseconds; defaults to `5000`
- `FANTASMA_WORKER_BATCH_SIZE`: worker batch size; defaults to `500`

Current Compose defaults:

- all three services get `postgres://fantasma:fantasma@postgres:5432/fantasma`
- `fantasma-ingest` gets `0.0.0.0:8081`
- `fantasma-api` gets `0.0.0.0:8082` and `FANTASMA_ADMIN_TOKEN=fg_pat_dev`
- `fantasma-worker` gets `FANTASMA_WORKER_POLL_INTERVAL_MS=5000`

## Startup Behavior

On startup, `fantasma-ingest`, `fantasma-api`, and `fantasma-worker` all:

- connect to Postgres using a pooled connection
- run SQL migrations from `crates/fantasma-store/migrations/`

They do not seed a project or create API keys at startup. Provisioning happens
through the operator management API after the stack is healthy.

There is no separate migration job in the default stack. The dashboard, when
enabled, serves static files from `apps/dashboard-web/public` through Nginx.

## Preconditions

Before bringing the stack up, confirm:

- enough free disk for Docker layers, Cargo build output, and Postgres data
- a healthy Docker daemon
- `docker`, `curl`, and `python3` are installed for the smoke/provisioning path

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

## Provision a Project

Once the stack is healthy, create a project and both scoped keys through the
management API:

```bash
PROVISIONED="$(./scripts/provision-project.sh \
  --project-name "Local Development" \
  --ingest-key-name "ios-sdk" \
  --read-key-name "dashboard")"
printf '%s\n' "$PROVISIONED"
```

Extract the returned secrets if you want shell variables for later commands:

```bash
INGEST_KEY="$(printf '%s' "$PROVISIONED" | python3 -c 'import json,sys; print(json.load(sys.stdin)["ingest_key"]["secret"])')"
READ_KEY="$(printf '%s' "$PROVISIONED" | python3 -c 'import json,sys; print(json.load(sys.stdin)["read_key"]["secret"])')"
```

Use the ingest key only with `POST /v1/events`. Use the read key only with
`/v1/metrics/*`.

## Smoke Verification

Run the preflighted smoke script:

```bash
./scripts/compose-smoke.sh
```

The smoke path:

- checks free disk and Docker availability
- runs the stack in a disposable Compose project
- waits for `http://localhost:8081/health` and `http://localhost:8082/health`
- provisions a project plus scoped ingest/read keys on the blank database
- ingests sample events
- polls worker-derived metrics until the expected data appears
- tears the stack down with volumes on exit

Set `FANTASMA_SMOKE_PROJECT_NAME` if you need a stable Compose project name for
debugging.

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

Provision a project:

```bash
PROVISIONED="$(./scripts/provision-project.sh \
  --project-name "Manual Checks" \
  --ingest-key-name "manual-ingest" \
  --read-key-name "manual-read")"
```

```bash
INGEST_KEY="$(printf '%s' "$PROVISIONED" | python3 -c 'import json,sys; print(json.load(sys.stdin)["ingest_key"]["secret"])')"
READ_KEY="$(printf '%s' "$PROVISIONED" | python3 -c 'import json,sys; print(json.load(sys.stdin)["read_key"]["secret"])')"
```

Ingest sample events:

```bash
curl -fsS -X POST http://localhost:8081/v1/events \
  -H "Content-Type: application/json" \
  -H "X-Fantasma-Key: ${INGEST_KEY}" \
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
curl -fsS "http://localhost:8082/v1/metrics/sessions/count/daily?start_date=2026-01-01&end_date=2026-01-02" \
  -H "X-Fantasma-Key: ${READ_KEY}"
```

```bash
curl -fsS "http://localhost:8082/v1/metrics/sessions/duration/total/daily?start_date=2026-01-01&end_date=2026-01-02" \
  -H "X-Fantasma-Key: ${READ_KEY}"
```

```bash
curl -fsS "http://localhost:8082/v1/metrics/events/aggregate?event=app_open&start_date=2026-01-01&end_date=2026-01-02&platform=ios&group_by=provider" \
  -H "X-Fantasma-Key: ${READ_KEY}"
```

```bash
curl -fsS "http://localhost:8082/v1/metrics/events/daily?event=app_open&start_date=2026-01-01&end_date=2026-01-02&group_by=provider" \
  -H "X-Fantasma-Key: ${READ_KEY}"
```

## Related Docs

- architecture and data flow: [`docs/architecture.md`](architecture.md)
- benchmark-specific commands: [`docs/performance.md`](performance.md)
- iOS demo walkthrough: [`apps/demo-ios/README.md`](../apps/demo-ios/README.md)
