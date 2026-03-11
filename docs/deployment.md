# Deployment

## Local Development Target

Fantasma should be runnable locally with:

```bash
docker compose up
```

The initial Compose setup should include:

- Postgres
- `fantasma-ingest`
- `fantasma-api`
- `fantasma-worker`
- optional `dashboard`

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

Service-specific examples:

- `FANTASMA_INGEST_BATCH_LIMIT`
- `FANTASMA_WORKER_POLL_INTERVAL_MS`
- `FANTASMA_API_TOKEN_TTL_HOURS`
