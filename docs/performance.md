# Performance

Fantasma now treats derived-metrics benchmarking as four explicit local suites instead of one window-first benchmark identity:

- `representative`: realistic live append traffic from many installs sending small blobs over time
- `mixed-repair`: the same live-shape traffic with a low late/out-of-order rate so repair-path regressions stay visible
- `stress`: the old large append/backfill/repair windows, kept for ceiling and regression visibility
- `reads-visibility`: grouped read-latency coverage, published separately from the worker-freshness gate

The harness is local/manual by design. Numeric benchmark publication is not a GitHub workflow.

## Default Gate

The default product-facing worker gate is `representative` plus `mixed-repair` together:

```bash
cargo run -p fantasma-bench -- \
  slo \
  --output-dir artifacts/performance/2026-03-15-derived-metrics-slo
```

That default run answers the worker question first:

- how derived freshness behaves under realistic append traffic
- whether a low but non-zero repair rate collapses freshness

Grouped public reads are no longer part of that worker gate. They still run in `reads-visibility`, but they do not drive worker-tuning decisions.

## Suite Selection

Use repeated `--suite <key>` flags when you want to run a specific family:

```bash
cargo run -p fantasma-bench -- \
  slo \
  --output-dir artifacts/performance/2026-03-15-derived-metrics-slo \
  --suite representative \
  --suite mixed-repair
```

Representative slice iteration can still target one concrete scenario:

```bash
cargo run -p fantasma-bench -- \
  slo \
  --output-dir artifacts/performance/2026-03-15-derived-metrics-slo \
  --scenario live-append-small-blobs
```

Publication-oriented reruns can override repetition for the worker-facing suites:

```bash
cargo run -p fantasma-bench -- \
  slo \
  --output-dir artifacts/performance/2026-03-15-derived-metrics-slo \
  --suite representative \
  --suite mixed-repair \
  --publication-repetitions 3
```

Supported SLO-specific overrides:

- `--suite <representative|mixed-repair|stress|reads-visibility>`
- `--scenario <key>`
- `--publication-repetitions`
- `--worker-session-batch-size`
- `--worker-session-incremental-concurrency`
- `--worker-session-repair-concurrency`
- `--worker-event-batch-size`

## Scenario Taxonomy

Representative worker gate:

- `live-append-small-blobs`
- `live-append-offline-flush`

Mixed-repair guardrail:

- `live-append-plus-light-repair`
- `live-append-plus-repair-hot-project`

Stress coverage:

- `stress-append-30d`, `stress-backfill-30d`, `stress-repair-30d`
- `stress-append-90d`, `stress-backfill-90d`, `stress-repair-90d`
- `stress-append-180d`, `stress-backfill-180d`, `stress-repair-180d`

Read visibility:

- `reads-visibility-30d`
- `reads-visibility-90d`
- `reads-visibility-180d`

Legacy mapping:

- old `append-30d/90d/180d` is now `stress-append-30d/90d/180d`
- old `backfill-30d/90d/180d` is now `stress-backfill-30d/90d/180d`
- old `repair-30d/90d/180d` is now `stress-repair-30d/90d/180d`
- old `reads-30d/90d/180d` is now `reads-visibility-30d/90d/180d`

## Workload Model

Representative and mixed-repair both use a fixed `30` day horizon, but the benchmark identity is now upload-shaped instead of giant-window-shaped.

Representative defaults:

- `1,000` active installs/day and `30` events/install/day, matching the current overall SLO scale baseline
- many installs with deterministic staggered upload order
- mostly small append blobs
- one explicit offline-flush scenario with medium bursts
- no real wall-clock sleeping; cadence is simulated through deterministic upload ordering
- sampled checkpoint freshness instead of phase-end-only freshness

Checkpoint policy:

- every repair blob is sampled
- every offline-flush blob is sampled
- normal append blobs sample every 20th append blob

Stress keeps the previous large append/backfill/repair windows for ceiling visibility. Reads-visibility keeps the grouped and ungrouped public query matrix for query-path visibility.

## Readiness And Reporting

Every scenario still publishes:

- `event_metrics_ready_ms`
- `session_metrics_ready_ms`
- `derived_metrics_ready_ms`

Representative and mixed-repair also publish checkpoint freshness summaries:

- checkpoint sample count
- checkpoint event p95
- checkpoint session p95
- checkpoint derived p95

Detailed checkpoint samples are written as a sidecar artifact beside the main scenario result. Stage B worker-attribution sidecars remain available for representative, mixed-repair, and stress scenarios.

## Gating

Default ship gate:

- both representative scenarios must pass
- both mixed-repair scenarios must pass

Primary worker signal:

- checkpoint `derived_metrics` readiness under representative append traffic
- checkpoint `derived_metrics` readiness under representative append-plus-repair traffic

Secondary worker context:

- event/session lane split
- repair backlog and lag shape
- dominant Stage B phase under mixed repair
- end-of-scenario readiness after the upload schedule drains

`stress` remains non-default ceiling coverage. `reads-visibility` remains non-default query coverage.

## Publication Rule

Product-facing publication must include `representative` and `mixed-repair` together.

`stress` and `reads-visibility` may publish separately when the goal is regression visibility rather than default worker promotion.

## Artifacts

The harness starts the benchmark stack in its own Compose project, renders a temporary benchmark Compose file with the selected worker settings, provisions a blank-database project plus scoped ingest/read keys, clears the target output directory before each run, and writes:

- `host.json`
- additive `run_config` in each scenario artifact
- one JSON and one Markdown file per scenario run
- `summary.json`
- `summary.md`
- checkpoint sidecars for representative and mixed-repair scenarios
- Stage B sidecars for representative, mixed-repair, and stress scenarios when trace data is present

If a scenario fails operationally, the suite still writes the failed run artifact plus the top-level summary before exiting non-zero.

Benchmark-only host ports:

- ingest: `http://127.0.0.1:18081`
- API: `http://127.0.0.1:18082`

Validate the benchmark stack before a run:

```bash
docker compose -f infra/docker/compose.bench.yaml config
```

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
