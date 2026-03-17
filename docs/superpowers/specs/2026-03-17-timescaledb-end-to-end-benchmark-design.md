# TimescaleDB End-to-End Benchmark Spike Design

> Outcome: retained as reference only. The March 17, 2026 branch-local spike was discarded after benchmarking, and Fantasma stayed on Postgres.

## Goal

Turn the TimescaleDB work into a real benchmark spike instead of a feasibility
gate by letting the Timescale benchmark backend run the same end-to-end
benchmark scenarios as the Postgres control while keeping the public API
unchanged.

## Product Boundary

The public API is the hard boundary for this spike.

Allowed to change for the Timescale benchmark backend:

- `events_raw` schema shape
- unique/primary indexes
- worker progress and replay semantics
- benchmark-only migrations/bootstrap
- benchmark-only query/index tuning

Not allowed to change:

- public routes
- public request/response payloads
- route auth requirements
- route-level query semantics for the benchmarked endpoints

Benchmark harness invariants:

- same scenario definitions
- same seeded data shape
- same readiness metrics
- same published benchmark artifact structure

If the Timescale backend needs a materially different internal design to work
well, that is acceptable in this spike. The point is to compare two backends
that implement the same public contract, not to preserve Postgres-oriented
internals.

## Scope

This spike should cover the same benchmark-visible workflow on both backends:

- benchmark stack bootstrap
- migrations / schema init
- seed ingest
- worker convergence to published readiness metrics
- read latency for the existing `reads-visibility-*` matrix, including
  raw-discovery routes

This spike should not stop at `events_raw` hypertable legality. If the current
schema is incompatible, the Timescale benchmark backend should replace it with a
Timescale-native version and adapt the worker path accordingly.

## Recommended Approach

Use a benchmark-only Timescale backend that is free to adopt a proper
hypertable-oriented raw-event design.

That means:

- keep the existing Postgres benchmark path as the control
- keep the public API identical across backends
- keep the benchmark CLI/backend selector serving the same scenarios and output
  schema across backends
- let the Timescale path create a different `events_raw` table/index set that
  satisfies hypertable rules
- let the Timescale path use a different worker checkpoint model if needed

The benchmark should answer:

- does Timescale improve or worsen seed ingest throughput?
- does Timescale improve or worsen worker readiness?
- does Timescale improve or worsen read latency on the same public queries?

## Internal Design Direction

### Raw Events

The Timescale benchmark backend should treat `events_raw` as a hypertable first,
not as a Postgres table with Timescale bolted on.

Expected direction:

- partition by event time
- use unique indexes that include the time partitioning column
- keep a stable tiebreaker for deterministic in-install ordering
- add the indexes needed for the measured queries rather than trying to retain
  every legacy Postgres index unchanged

The exact identity shape can differ from Postgres as long as it supports the
same externally visible behavior.

### Worker Progress

The current worker depends on a global raw-id frontier. That is a Postgres
design constraint, not a spike requirement.

For the Timescale backend, the worker may instead use a benchmark-only progress
model such as:

- ordered scans by `(timestamp, id)` or another deterministic composite cursor
- backend-specific worker offset rows
- backend-specific repair fetches keyed by the same ordering contract

The worker still needs to preserve the same externally visible derived-metric
results, but it does not need to preserve the exact existing checkpoint
implementation.

### Comparability

Comparability must be preserved at the benchmark boundary:

- same seeded datasets
- same scenarios
- same public queries
- same readiness metrics
- same result artifacts

Internal implementation differences are acceptable only when they stay behind
that benchmark boundary.

## Non-Goals

- no attempt to keep Timescale fully migration-compatible with the current
  default local stack
- no requirement that Timescale share the same raw-event primary key contract as
  Postgres
- no production rollout or deployment recommendation in this spike
- no public API expansion
- no dashboard-specific query work beyond the existing benchmark matrix

## Required Documentation Updates

Any implementation of this design must update:

- `docs/STATUS.md`
- `docs/performance.md`
- `docs/deployment.md`
- the existing Timescale implementation plan under `docs/superpowers/plans/`

The status entry should explicitly say the spike moved from "hypertable legality
probe" to "benchmark-only alternate backend design".

## Acceptance Criteria

Success for this spike means:

- the Timescale benchmark backend boots in the benchmark stack
- the Timescale benchmark backend runs the same `reads-visibility-30d` scenario
  end to end as the Postgres control
- benchmark artifacts exist for both Postgres and Timescale for
  `reads-visibility-30d` using the same output structure
- the public API surface and route semantics used by that scenario stay
  unchanged across both backends
- the comparison includes ingest throughput, readiness metrics, and read
  latencies

Failure for this spike means:

- the team attempted a proper Timescale-native internal design for the benchmark
  backend
- but the Timescale backend still could not complete the comparable
  `reads-visibility-30d` scenario end to end

That is still a valid spike result, but it is not considered a successful
benchmark implementation.
