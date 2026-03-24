# 👻 Fantasma 

A self-hosted, privacy-first, high-performance mobile analytics platform.

Fantasma favors a small, legible product surface over analytics sprawl. It is
for teams that want event, session, and usage insight without drifting into ad
tech or profile building. Privacy is enforced in the product shape: no
person-level identity, no hidden enrichment, and intentionally narrow event
context.

We are inspired by [usefathom](https://usefathom.com)'s opinionated, privacy-first approach.

## What Fantasma Is Not (and will never be)

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
- an operator-facing CLI for self-hosted management and metrics access
- operator-provisioned projects with separate ingest and read keys
- background aggregation workers
- durable iOS, Android, Flutter, and React Native SDKs
- simple self-hosted deployment

The API remains the primary interface. The CLI is the primary operator
workflow for self-hosted instances.

Run `cargo run -p fantasma-cli -- --help` for the operator CLI entry point.

## Learn More

This README is the canonical public product statement. Keep it high-level.
Technical and operational details belong in the docs below.

- Deployment: [`docs/deployment.md`](docs/deployment.md)
- Architecture: [`docs/architecture.md`](docs/architecture.md)
- iOS docs: [`sdks/ios/README.md`](sdks/ios/README.md)
- Android docs: [`sdks/android/README.md`](sdks/android/README.md)
- Flutter docs: [`sdks/flutter/README.md`](sdks/flutter/README.md)
- React Native docs: [`sdks/react-native/README.md`](sdks/react-native/README.md)
- Project status: [`docs/STATUS.md`](docs/STATUS.md)
- Contributor rules: [`AGENTS.md`](AGENTS.md)

## License

Fantasma is available under the [MIT License](LICENSE).
