# Fantasma 👻

Fantasma is a self-hosted, privacy-first, high-performance mobile analytics platform.

Fantasma favors a small, legible product surface over analytics sprawl. It is
for teams that want event, session, and usage insight without drifting into ad
tech or profile building. Privacy is enforced in the product shape: no
person-level identity, no hidden enrichment, and intentionally narrow event
context.

Fantasma is inspired by Fathom's opinionated, privacy-first approach, but
built for mobile analytics.

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

- Deployment: [`docs/deployment.md`](docs/deployment.md)
- Architecture: [`docs/architecture.md`](docs/architecture.md)
- Project status: [`docs/STATUS.md`](docs/STATUS.md)
- Contributor rules: [`AGENTS.md`](AGENTS.md)
