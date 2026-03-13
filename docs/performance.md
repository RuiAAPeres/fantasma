# Performance

Fantasma now publishes derived-metrics performance through one fixed local suite:

```bash
cargo run -p fantasma-bench -- \
  slo \
  --output-dir artifacts/performance/2026-03-13-derived-metrics-slo
```

This slice separates two questions that were previously mixed together:

- freshness: how long it takes the worker to make derived data queryable after raw ingest completes
- read latency: how fast the public metrics routes answer once the derived data is already ready

The suite is local/manual by design. Numeric benchmark publication is not a GitHub workflow.

## Guardrails

Deterministic boundedness checks still live in the normal Rust test suite.

Representative local commands:

```bash
./scripts/docker-test.sh -p fantasma-store --lib
```

```bash
./scripts/docker-test.sh -p fantasma-worker --lib
```

```bash
cargo test -p fantasma-bench --quiet
```

## SLO Suite

Run the publishable suite from the repository root:

```bash
cargo run -p fantasma-bench -- \
  slo \
  --output-dir artifacts/performance/2026-03-13-derived-metrics-slo
```

The harness starts the benchmark stack in its own Compose project, provisions a blank-database project plus scoped ingest/read keys, clears the target output directory before each run, drives the fixed workload, and writes:

- `host.json`
- one JSON and one Markdown file per scenario run
- `median.json` and `median.md` for repeated `30d` scenarios
- top-level `summary.json`
- top-level `summary.md`

If a scenario fails operationally, the suite still writes the failed run artifact plus the top-level summary before exiting non-zero.

Benchmark-only host ports:

- ingest: `http://127.0.0.1:18081`
- API: `http://127.0.0.1:18082`

Validate the benchmark stack before a run:

```bash
docker compose -f infra/docker/compose.bench.yaml config
```

## Workload Model

The `slo` suite does not accept workload knobs. It always uses:

- `30` events per day per install
- `1` session per day per install
- `1,000` active installs per day

Window sizes:

| Window | Days | Events |
| --- | ---: | ---: |
| `30d` | 30 | 900,000 |
| `90d` | 90 | 2,700,000 |
| `180d` | 180 | 5,400,000 |

Traffic shape:

- append workloads ingest days in chronological order
- backfill workloads ingest reverse chronological day chunks while preserving in-day ordering
- repair workloads seed the ordered dataset first, then inject older same-day events for a bounded subset of install-days
- read workloads seed once, wait for derived readiness, then execute the full read matrix against already-derived data

## Scenarios

The suite always runs these scenarios:

- `append-30d`, `backfill-30d`, `repair-30d`
- `append-90d`, `backfill-90d`, `repair-90d`
- `append-180d`, `backfill-180d`, `repair-180d`
- `reads-30d`, `reads-90d`, `reads-180d`

Repetition policy:

- `30d` scenarios run `3` times and publish medians
- `90d` and `180d` scenarios run once and publish the raw result

Visibility-oriented freshness timeouts:

- `30d`: `5m`
- `90d`: `20m`
- `180d`: `40m`

For append/backfill/repair, `90d` and `180d` timeout-shaped readiness is still published even though freshness is visibility-only in this slice.

For `reads-90d` and `reads-180d`, the same timeout-shaped readiness is published as visibility-only context, and the suite then continues waiting for fully derived data before it runs the read matrix.

## Readiness Metrics

Every scenario records family-specific readiness:

- `event_metrics_ready_ms`
- `session_metrics_ready_ms`
- `derived_metrics_ready_ms = max(event_metrics_ready_ms, session_metrics_ready_ms)`

Interpretation:

- event readiness proves the event cuboids are queryable
- session readiness proves sessionization, session rollups, and `new_installs` are queryable
- derived readiness is the user-facing freshness ceiling for the whole derived metrics surface
- readiness always means worker catch-up lag after the scenario's raw ingest phase completed; ingest throughput is published separately in the phase measurements
- for `repair-*`, readiness is measured after the late-event repair ingest finishes, not from the seed load
- for `reads-30d`, readiness must complete before the read matrix runs
- for `reads-90d` and `reads-180d`, the published readiness numbers are visibility-only; if they hit the visibility timeout, the suite keeps waiting for fully derived data before it measures read latency

## Query Matrix

The suite records both grouped and ungrouped public reads. Grouped reads are the hard gate.

Hard-gated grouped queries:

- event `count` `day` grouped by `provider,region`
- event `count` `hour` grouped by `provider,region`
- session `count` `day` grouped by `platform,app_version`
- session `count` `hour` grouped by `platform,app_version`
- session `duration_total` `day` grouped by `platform,app_version`
- session `duration_total` `hour` grouped by `platform,app_version`
- session `new_installs` `day` grouped by `platform,app_version`
- session `new_installs` `hour` grouped by `platform,app_version`

Visibility-only reads:

- the ungrouped equivalents of every query above

Query measurement policy:

- `10` warmups
- `100` timed iterations
- publish min, p50, p95, and max

## Hard Budgets

Freshness gates in this slice:

| Scenario | `event_metrics_ready_ms` | `session_metrics_ready_ms` | `derived_metrics_ready_ms` |
| --- | ---: | ---: | ---: |
| `append-30d` | `<= 30_000` | `<= 60_000` | `<= 60_000` |
| `backfill-30d` | `<= 30_000` | `<= 60_000` | `<= 60_000` |
| `repair-30d` | `<= 30_000` | `<= 60_000` | `<= 60_000` |

Grouped-read p95 gates:

| Window | Day | Hour |
| --- | ---: | ---: |
| `30d` | `<= 100ms` | `<= 150ms` |
| `90d` | `<= 200ms` | `<= 300ms` |
| `180d` | `<= 350ms` | `<= 500ms` |

Freshness is visibility-only for `90d` and `180d`. Read budgets remain hard gates for all windows.

## Reading Results

Use the published results like this:

- If `append-30d`, `backfill-30d`, or `repair-30d` misses freshness, the worker is the bottleneck. The next slice should focus on worker/session-repair internals or indexing, not more benchmark reshaping.
- If freshness passes but `reads-*` misses grouped-read budgets, the derived tables are ready fast enough and the problem is on the query/read path.
- If event readiness is fast but session readiness is slow, the session lane is the limiter.
- If session readiness is fast but event readiness is slow, the event cuboid lane is the limiter.

## Legacy Commands

The older manual commands still exist for focused investigation:

```bash
cargo run -p fantasma-bench -- stack --scenario hot-path --profile ci --output artifacts/performance/hot-path.json
```

```bash
cargo run -p fantasma-bench -- series --profile heavy --repetitions 5 --output-dir artifacts/performance/2026-03-13-m3-pro-heavy
```

They are no longer the primary publication path for derived freshness/read SLAs. Use `slo` when the goal is decision-quality freshness and read-budget evidence.
