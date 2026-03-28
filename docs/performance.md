# Performance

Fantasma now treats derived-metrics benchmarking as five explicit local suites instead of one window-first benchmark identity:

- `representative`: realistic live append traffic from many installs sending small blobs over time
- `mixed-repair`: the same live-shape traffic with a low late/out-of-order rate so repair-path regressions stay visible
- `burst-readiness`: small realistic bursts that answer the customer question directly: "if a few hundred events land now, how long until sessions show up?"
- `stress`: the old large append/backfill/repair windows, kept for ceiling and regression visibility
- `reads-visibility`: grouped read-latency coverage, published separately from the worker-freshness gate

The harness is local/manual by design. Numeric benchmark publication is not a GitHub workflow.

## Modes

`fantasma-bench slo` now has two explicit execution modes:

- `iterative`: fast, directional, and the default CLI path
- `publication`: slower, richer, and the only mode allowed to produce product-facing benchmark artifacts

Iterative mode keeps the same 30-day representative identity, but it is not a publication-quality benchmark. Treat it as a better/flat/worse worker iteration screen.

## Default Gate

The default iterative worker gate is the canonical representative plus mixed-repair pair:

```bash
cargo run -p fantasma-bench -- \
  slo \
  --output-dir artifacts/performance/2026-03-15-derived-metrics-slo
```

That default iterative run answers the worker question first:

- how derived freshness behaves under realistic append traffic
- whether a low but non-zero repair rate collapses freshness
- whether the current branch looks better, flat, or worse against the pinned iterative baseline

Grouped public reads are no longer part of that worker gate. They still run in `reads-visibility`, but they do not drive worker-tuning decisions.

The default iterative pair is:

- `live-append-small-blobs`
- `live-append-plus-light-repair`

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

Publication-oriented reruns should switch mode explicitly and may override repetition for the worker-facing suites:

```bash
cargo run -p fantasma-bench -- \
  slo \
  --output-dir artifacts/performance/2026-03-15-derived-metrics-slo \
  --mode publication \
  --suite representative \
  --suite mixed-repair \
  --publication-repetitions 3
```

Supported SLO-specific overrides:

- `--mode <iterative|publication>`
- `--suite <representative|mixed-repair|burst-readiness|stress|reads-visibility>`
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

Burst readiness:

- `burst-readiness-300-installs-x1`
- `burst-readiness-150-installs-x2`
- `burst-readiness-100-installs-x3`

Stress coverage:

- `stress-append-30d`, `stress-backfill-30d`, `stress-repair-30d`
- `stress-append-90d`, `stress-backfill-90d`, `stress-repair-90d`
- `stress-append-180d`, `stress-backfill-180d`, `stress-repair-180d`

Read visibility:

- `reads-visibility-30d`
- `reads-visibility-90d`
- `reads-visibility-180d`

The read-visibility matrix now includes grouped D2 reads for events plus
session `count`, `duration_total`, and `new_installs`. Day-level
dimension-aware `active_installs` reads remain visibility-only and are not
hard gates, because filtered and grouped active-install slices may overlap by
design.

Legacy mapping:

- old `append-30d/90d/180d` is now `stress-append-30d/90d/180d`
- old `backfill-30d/90d/180d` is now `stress-backfill-30d/90d/180d`
- old `repair-30d/90d/180d` is now `stress-repair-30d/90d/180d`
- old `reads-30d/90d/180d` is now `reads-visibility-30d/90d/180d`

## Workload Model

Representative and mixed-repair both use a fixed `30` day horizon, but the benchmark identity is now upload-shaped instead of giant-window-shaped.

Representative defaults are now mode-specific:

- `iterative`
  - `250` installs/day
  - directional-only checkpoint sampling
  - intended for repeated local worker iteration
- `publication`
  - `1,000` installs/day and `30` events/install/day, matching the current full representative scale baseline
  - publication-grade checkpoint density and suite coverage
  - intended for artifact-quality publication and promotion decisions

Iterative checkpoint policy:

- append blobs sample every `200th`
- offline-flush blobs sample every `25th`
- repair blobs sample every `10th`
- end-of-scenario readiness still runs for every scenario

Publication checkpoint policy:

- every repair blob is sampled
- every offline-flush blob is sampled
- normal append blobs sample every `20th` append blob

Burst-readiness uses the same query/readiness machinery, but the upload schedule is deliberately tiny and exact:

- `burst-readiness-300-installs-x1`: `300` installs each send `1` event, chunked as `3 x 100` ingest requests
- `burst-readiness-150-installs-x2`: `150` installs each send `2` events, chunked as `3 x 100` ingest requests
- `burst-readiness-100-installs-x3`: `100` installs each send `3` events, chunked as `3 x 100` ingest requests

These runs are not the worker default gate. They exist to answer a more realistic product-freshness question than the large stress windows.

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

Iterative summaries also print a compact verdict block from the pinned baseline:

- representative append: `better|flat|worse`
- representative checkpoint derived: `better|flat|worse`
- representative final derived: `better|flat|worse`
- mixed repair: `better|flat|worse`
- mixed repair checkpoint derived: `better|flat|worse`
- mixed repair final derived: `better|flat|worse`
- dominant Stage B phase: `changed|unchanged`
- overall verdict: `better|flat|worse`

## Gating

Default iterative ship gate:

- `live-append-small-blobs`
- `live-append-plus-light-repair`

Primary worker signal:

- checkpoint `derived_metrics` readiness under representative append traffic
- checkpoint `derived_metrics` readiness under representative append-plus-repair traffic

Secondary worker context:

- event/session lane split
- repair backlog and lag shape
- dominant Stage B phase under mixed repair
- end-of-scenario readiness after the upload schedule drains

`burst-readiness` remains a non-default customer-freshness probe. `stress` remains non-default ceiling coverage. `reads-visibility` remains non-default query coverage.

## Publication Rule

Product-facing publication must include `representative` and `mixed-repair` together.

`stress` and `reads-visibility` may publish separately when the goal is regression visibility rather than default worker promotion.

The pinned iterative baseline is checked in under `crates/fantasma-bench/baselines/`. Refresh it only when the team intentionally accepts a new performance baseline or changes benchmark policy. Baseline refresh is explicit and checked in; routine iterative runs never rewrite it automatically. Keep baseline provenance actionable: point it at a retained repo path or documented status entry, not an ephemeral local `/tmp` artifact path.

## Artifacts

The harness starts the benchmark stack in its own Compose project, renders a temporary benchmark Compose file with the selected worker settings, provisions a blank-database project plus scoped ingest/read keys, clears the target output directory before each run, and writes:

- `host.json` with coarse environment metadata only
- additive `run_config` in each scenario artifact
- one JSON and one Markdown file per scenario run
- `summary.json`
- `summary.md`
- compact sidecars such as `checkpoint-readiness.json` or `stage-b.json` when a scenario emits them

Heavy raw diagnostic sidecars such as `append-attribution.json`, `event-lane-trace.jsonl`, and `stage-b-trace.jsonl` are for local investigation only. Do not check them in as published benchmark artifacts; keep the retained repo artifact set compact and summary-oriented.

## Demo Stack Burst Workflow

When the goal is to measure realistic customer-facing freshness on an already-running demo stack, do not use the isolated `fantasma-bench` Compose path. That path binds the benchmark stack to the local benchmark ports and will collide with already-running services on the host.

Use the checked-in remote workflow instead:

```bash
./scripts/run-demo-stack-burst-bench.sh --host user@example-host
```

That wrapper copies [demo-stack-burst-bench.py](/Users/ruiperes/Code/fantasma/scripts/demo-stack-burst-bench.py) to the remote host, reads the remote `.env.demo`, derives the demo loopback ports from `FANTASMA_API_PORTS` and `FANTASMA_INGEST_PORTS`, and benchmarks the three realistic `300`-event burst scenarios directly against the existing demo stack:

- `burst-readiness-300-installs-x1`
- `burst-readiness-150-installs-x2`
- `burst-readiness-100-installs-x3`

This is the benchmark path to use for real demo-stack freshness checks. Keep the isolated `fantasma-bench slo` Compose path for local benchmark-stack runs and for the larger stress suites.
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
