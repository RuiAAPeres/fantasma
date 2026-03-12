# Fantasma

Fantasma is a self-hosted, privacy-first mobile analytics platform.

Fantasma helps mobile teams understand app activity without turning analytics
into identity infrastructure.

It is intentionally narrow:

- mobile-first, not web analytics retrofitted onto apps
- privacy-first, with install-scoped analytics instead of person-level tracking
- API-first, so dashboards and integrations use the same public surface
- self-hosted, with simple deployment and minimal moving parts

Fantasma favors a small, legible product surface over analytics sprawl. It is
for teams that want event, session, and usage insight without drifting into ad
tech, hidden enrichment, or profile building. It keeps event context
intentionally narrow so the analytics surface stays predictable and fast to run.

## What Fantasma Is Not

Fantasma is not trying to be a full product analytics suite. It does not target:

- attribution
- ad tracking
- A/B testing
- session replay
- customer data platform behavior
- fingerprinting

## Current Shape

Today Fantasma is centered on:

- a Rust backend and public API
- background aggregation workers
- durable mobile SDKs
- simple self-hosted deployment

The dashboard is secondary and should consume the same public API as everything
else.

## Learn More

This README is the canonical public product statement. Keep it high-level.
Technical and operational details belong in the docs below.

- Deployment: [`docs/deployment.md`](/Users/ruiperes/Code/fantasma/docs/deployment.md)
- Architecture: [`docs/architecture.md`](/Users/ruiperes/Code/fantasma/docs/architecture.md)
- Project status: [`docs/STATUS.md`](/Users/ruiperes/Code/fantasma/docs/STATUS.md)
- Contributor rules: [`AGENTS.md`](/Users/ruiperes/Code/fantasma/AGENTS.md)
