# Performance

Fantasma now backs its bounded-performance claims with two layers of proof:

- deterministic Rust regressions in normal CI
- numeric Docker-stack benchmarks on `main` and manual runs

## Deterministic Guardrails

PR CI picks up these checks through the normal Rust test suite:

- event-metrics rollup fanout stays bounded for a max-dimension event payload
- out-of-order session repair still rebuilds only touched UTC start days
- representative dim2 and dim3 event-metrics reads stay on the dedicated Postgres read indexes instead of falling back to sequential scans

The new planner checks live in the DB-backed Rust test path, so run them through Docker when you want repo-faithful local verification:

```bash
./scripts/docker-test.sh -p fantasma-store event_metrics_dim2_reads_use_bounded_read_indexes
```

```bash
./scripts/docker-test.sh -p fantasma-store event_metrics_dim3_reads_use_bounded_read_indexes
```

```bash
cargo test -p fantasma-worker event_metrics_rollups_stay_bounded
```

## Stack Benchmarks

Run the benchmark harness from the repository root:

```bash
cargo run -p fantasma-bench -- \
  stack \
  --scenario hot-path \
  --profile ci \
  --output artifacts/performance/hot-path.json
```

```bash
cargo run -p fantasma-bench -- \
  stack \
  --scenario repair-path \
  --profile ci \
  --output artifacts/performance/repair-path.json
```

Each run writes:

- a JSON result file at the requested `--output` path
- a sibling Markdown summary with the same basename and `.md` extension

## Workloads

`hot-path`

- drives 600 max-dimension `app_open` events across 200 installs through `POST /v1/events`
- waits for worker-derived event and session metrics to match expected values
- records ingest throughput, derive lag, and warmed query latency for the public metrics routes

`repair-path`

- seeds 250 events that create non-contiguous day buckets and multiple split sessions
- sends 100 late events that force exact-day session repair on touched days only
- records seed ingest throughput, repair ingest throughput, repair lag, and warmed event/session query latency

`scale-path`

- drives a larger bounded dataset through the same public ingest and query surface over a 30-day window
- uses the same capped dimension model as production claims, but with far more rows than the hot-path and repair-path checks
- records ingest throughput, derive lag, and warmed query latency over the wider window so public “fast to run” claims are backed by more than tiny slices

## Budget Model

- `ci` uses enforced thresholds from [`crates/fantasma-bench/budgets/ci.json`](/Users/ruiperes/Code/fantasma/crates/fantasma-bench/budgets/ci.json)
- `extended` runs the same scenarios but skips threshold enforcement and is intended for manual investigation
- benchmark runs use [`infra/docker/compose.bench.yaml`](/Users/ruiperes/Code/fantasma/infra/docker/compose.bench.yaml), which keeps the normal stack topology but lowers the worker poll interval to `50ms` and raises the worker batch size to `1000`
- budget tightening should use GitHub runner medians from repeated workflow runs, not a single local-machine benchmark

## Automation

GitHub Actions runs the numeric benchmark workflow on pushes to `main` and on `workflow_dispatch`:

- workflow: [`performance.yml`](/Users/ruiperes/Code/fantasma/.github/workflows/performance.yml)
- artifacts: per-scenario JSON and Markdown summaries uploaded from `artifacts/performance/`
