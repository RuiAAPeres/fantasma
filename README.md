# Fantasma

Fantasma is a self-hosted, privacy-first mobile analytics platform.

The project is intentionally narrow:

- event-based analytics for mobile apps
- API-first access to metrics
- durable mobile SDKs
- simple self-hosted deployment
- an optional dashboard built on the same public API

Fantasma is not trying to be a full product analytics suite. It does not target feature flags, attribution, experimentation, session replay, or customer data platform workflows.

## MVP Shape

The current MVP target is:

- Rust backend
- Postgres-backed event storage and aggregates
- public query API
- durable iOS SDK
- Docker Compose local deployment

The dashboard is secondary and should consume the same API as external clients.

## Repository Layout

```text
fantasma/
  apps/
    dashboard-web/
    demo-ios/
    demo-android/
  crates/
    fantasma-api/
    fantasma-auth/
    fantasma-core/
    fantasma-ingest/
    fantasma-worker/
  docs/
  infra/
    docker/
  schemas/
    events/
    openapi/
  sdks/
    ios/
    android/
```

## Documentation

Documentation is part of the product surface. Work is not complete unless the relevant docs move with it.

- Repository rules: [`AGENTS.md`](/Users/ruiperes/Code/fantasma/AGENTS.md)
- Project memory and progress log: [`docs/STATUS.md`](/Users/ruiperes/Code/fantasma/docs/STATUS.md)
- Architecture notes: [`docs/architecture.md`](/Users/ruiperes/Code/fantasma/docs/architecture.md)
- Deployment notes: [`docs/deployment.md`](/Users/ruiperes/Code/fantasma/docs/deployment.md)

## Development

This repository is being bootstrapped as a Cargo workspace. The first implementation slices focus on:

1. shared schemas and core types
2. auth and project/key modeling
3. ingest service
4. background aggregation worker
5. query API
6. durable iOS SDK

## Commit Discipline

Keep commits small and focused. Each commit should represent one coherent change and be reversible without collateral cleanup.
