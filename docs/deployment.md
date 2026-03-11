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
- Service images: `infra/docker/Dockerfile.ingest`, `infra/docker/Dockerfile.api`, `infra/docker/Dockerfile.worker`

## Deployment Principles

- Prefer a small number of services.
- Avoid optional infrastructure in v1 unless required by a concrete bottleneck.
- Make it easy to run locally and in a simple self-hosted environment.
- Keep configuration explicit through environment variables.

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

## Startup Bootstrap

On startup, `fantasma-ingest` and `fantasma-api` both:

- connect to Postgres using a pooled connection
- create `projects`, `api_keys`, and `events_raw` if they do not exist
- create the initial raw-event indexes
- seed the local development project plus ingest key from environment variables

This keeps the first vertical slice self-contained without separate migration tooling.

## Local Smoke Test

Start the stack:

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
        "app_version": "1.0"
      }
    ]
  }'
```

Query the count:

```bash
curl "http://localhost:8082/v1/metrics/events/count?project_id=9bad8b88-5e7a-44ed-98ce-4cf9ddde713a&start=2026-01-01T00:00:00Z&end=2026-01-02T00:00:00Z" \
  -H "Authorization: Bearer fg_pat_dev"
```

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

Use the existing metrics endpoint to confirm ingestion after interacting with the app:

```bash
curl "http://localhost:8082/v1/metrics/events/count?project_id=9bad8b88-5e7a-44ed-98ce-4cf9ddde713a&start=2026-01-01T00:00:00Z&end=2027-01-01T00:00:00Z" \
  -H "Authorization: Bearer fg_pat_dev"
```
