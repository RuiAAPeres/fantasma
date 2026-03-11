# AGENTS.md

This repository is for building Fantasma, a privacy-first, self-hosted mobile analytics stack. Treat this file as the operating manual for coding agents and contributors.

## Product Rules

- Keep Fantasma simple, API-first, mobile-first, self-hosted, and privacy-first.
- The API is the primary interface. The dashboard is secondary.
- Everything is an event.
- Events are immutable.
- Client-side durability is required.
- Backend aggregation is asynchronous.
- Prefer deployability and clarity over infrastructure complexity.

## MVP Defaults

Until the repository says otherwise, assume the following:

- Postgres-only v1
- project-scoped multi-tenant model
- backend + public API + durable iOS SDK are the MVP
- dashboard is optional and must consume the public API
- no Redis, Kafka, or extra operational dependencies in v1

## Explicit Non-Goals

Do not add scope beyond the product vision without an explicit decision:

- feature flags
- attribution
- ad tracking
- A/B testing
- session replay
- fingerprinting
- customer data platform behavior

## Backend Rules

- Ingestion happens through `POST /v1/events`.
- Query endpoints live under `/v1/metrics/*`.
- Raw events are stored append-only.
- Workers own aggregate generation.
- Do not perform synchronous aggregation in the ingest path.
- Do not add hidden enrichment or automatic identity stitching.
- Scope all persisted data by `project_id`.

## SDK Rules

- Keep the SDK API minimal and explicit.
- No swizzling.
- No automatic screen tracking.
- No hidden behavior.
- Persist tracked events before upload.
- `clear()` rotates local identity without deleting already-queued events.

## Documentation Rules

Documentation is a first-class deliverable.

- Any public API change must update `schemas/openapi`.
- Any event contract change must update `schemas/events`.
- Any architecture change must update `docs/architecture.md`.
- Any deployment change must update `docs/deployment.md`.
- Public-facing SDK behavior must include usage examples in docs.
- Work is not complete until documentation is updated.

## Project Memory

Keep a running record of what changed and what is next.

- Update `docs/STATUS.md` when significant work starts.
- Update `docs/STATUS.md` when significant work finishes.
- Record open decisions, active work, and next steps.
- Use commit history for fine-grained change history and `docs/STATUS.md` for current project memory.

## Agent Workflow

Fantasma now expects Codex agents to use the installed `superpowers` skills when they apply. Repository rules in this file still win over any skill defaults.

- Use `writing-plans` before starting multi-step feature or refactor work. Save plans under `docs/superpowers/plans/` unless the task clearly does not need one.
- Use `subagent-driven-development` when executing a plan with separable tasks in the current session.
- Use `systematic-debugging` for defects, flaky tests, or unexpected runtime behavior instead of guessing at fixes.
- Use `verification-before-completion` before claiming work is done, fixed, or passing. Do not report success without fresh command output.
- Use `requesting-code-review` or `receiving-code-review` when preparing or responding to substantive review cycles.
- Use `finishing-a-development-branch` before final handoff when the task includes branch cleanup, verification, and merge-readiness work.
- Keep superpowers usage pragmatic: choose the smallest applicable workflow, but do not skip a clearly relevant skill.

## Build Order

When starting from a thin repository, follow this order unless there is a clear reason not to:

1. workspace bootstrap and docs scaffolding
2. shared schemas and OpenAPI
3. core types and auth model
4. ingest service
5. worker and aggregates
6. query API
7. iOS SDK
8. optional dashboard

## Engineering Rules

- Favor typed crate-level errors with `thiserror` in libraries.
- Reserve `anyhow` for binaries.
- Prefer simple interfaces over clever abstractions.
- Add tests for validation, auth, aggregation idempotency, and SDK durability.
- Keep comments short and explain why, not what.

## Commit Discipline

- Keep commits small and single-purpose.
- Commit after each coherent milestone.
- Do not mix refactors with feature work unless they are inseparable.
- Use clear imperative commit messages.
- Add commit bodies when the design tradeoff is not obvious.
- Well-documented commits are part of the repository memory.
