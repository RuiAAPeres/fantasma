# Performance

Fantasma keeps performance proofing in two places:

- deterministic regressions in normal CI
- local stack benchmarks through `fantasma-bench`

The heavyweight numeric benchmark workflow is intentionally gone from GitHub Actions. Numeric benchmark runs are now an explicit local/manual operation.

## Deterministic Guardrails

Normal CI still carries the fast boundedness checks through the Rust test suite:

- event-metrics rollup fanout stays bounded for a max-dimension event payload
- out-of-order session repair still rebuilds only touched UTC start days
- representative dim2 and dim3 event-metrics reads stay on the dedicated Postgres read indexes instead of falling back to sequential scans

Run the DB-backed store checks through Docker when you want repo-faithful local verification:

```bash
./scripts/docker-test.sh -p fantasma-store event_metrics_dim2_reads_use_bounded_read_indexes
```

```bash
./scripts/docker-test.sh -p fantasma-store event_metrics_dim3_reads_use_bounded_read_indexes
```

```bash
cargo test -p fantasma-worker max_dimension_event_metrics_fanout_stays_bounded
```

## Stack Benchmarks

Run a single benchmark scenario from the repository root:

```bash
cargo run -p fantasma-bench -- \
  stack \
  --scenario hot-path \
  --profile ci \
  --output artifacts/performance/hot-path.json
```

Single-scenario runs write:

- a JSON result file at the requested `--output` path
- a sibling Markdown summary with the same basename and `.md` extension

For publishable local investigations, run the repeated heavy series:

```bash
cargo run -p fantasma-bench -- \
  series \
  --profile heavy \
  --repetitions 5 \
  --output-dir artifacts/performance/2026-03-13-m3-pro-heavy
```

Series runs write:

- `host.json` with CPU/RAM/OS/arch context
- per-scenario raw run JSON and Markdown files
- per-scenario median JSON and Markdown files
- a top-level `summary.json` and `summary.md`

The benchmark harness runs the stack in its own Compose project (`fantasma-bench`) and uses benchmark-only host ports:

- ingest: `http://127.0.0.1:18081`
- API: `http://127.0.0.1:18082`

That keeps local benchmark runs from tearing down or colliding with the default development stack on `8081` / `8082`.

Each run provisions its own blank-database project plus scoped ingest/read keys through the operator management API before driving traffic.

## Profiles

`ci`

- smaller bounded workload
- still evaluates the committed thresholds from [`crates/fantasma-bench/budgets/ci.json`](../crates/fantasma-bench/budgets/ci.json)
- useful for quick local checks

`extended`

- larger manual workload
- no threshold enforcement
- intended for ad hoc local investigation

`heavy`

- publishable local workload
- runs `hot-path`, `repair-path`, and `scale-path` at materially larger sizes
- intended to be repeated and summarized with medians instead of treated as a single-run truth source

## Workloads

`hot-path`

- drives bounded max-dimension `app_open` traffic through `POST /v1/events`
- waits for worker-derived event and session metrics to match expected values
- records ingest throughput, derive lag, and warmed query latency for the public metrics routes

`repair-path`

- seeds non-contiguous day buckets and multiple split sessions
- sends late events that force exact-day session repair on touched days only
- records seed ingest throughput, repair ingest throughput, repair lag, and warmed event/session query latency

`scale-path`

- drives a larger bounded dataset through the same public ingest and query surface over a wider window
- uses the same capped dimension model as the smaller checks, but with far more rows
- records ingest throughput, derive lag, and warmed query latency over the broader range

## Benchmark Stack

Benchmark runs use [`infra/docker/compose.bench.yaml`](../infra/docker/compose.bench.yaml), which keeps the normal stack topology but runs under the dedicated `fantasma-bench` Compose project, exposes benchmark-only host ports, lowers the worker poll interval to `50ms`, and raises the worker batch size to `1000`.

Before a publishable run, validate the Compose file and make sure Docker itself is reachable:

```bash
docker compose -f infra/docker/compose.bench.yaml config
```

```bash
docker info
```
