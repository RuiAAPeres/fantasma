# Session-Lane Freshness Optimization Pass After Dim4

## Summary

- Optimize freshness, not reads. The current dim4 read path already passes comfortably; the visible latency ceiling is the session lane in append and backfill, with repair kept correct and non-regressing.
- Keep the public API, metrics contract, and bounded event/session models unchanged.
- Make the pass staged and cheap first: add real benchmark config injection plus additive run-config publication, run a small session-side screening sweep, prove only finalists on larger scenarios, and only authorize one structural optimization if the measurements clearly point to a single dominant phase.
- Update `docs/STATUS.md` when the work starts and again when it lands. Promote any winning worker settings into normal defaults and both Compose files.

## Implementation Changes

- Benchmark config injection and additive artifact recording:
  - Add explicit worker-config overrides to the `slo` command: `--worker-session-batch-size`, `--worker-session-incremental-concurrency`, `--worker-session-repair-concurrency`, `--worker-event-batch-size`, and `--repetitions-30d`.
  - Keep current behavior as the default when flags are omitted: `session_batch_size=1000`, `event_batch_size=5000`, `session_incremental_concurrency=8`, `session_repair_concurrency=2`, `repetitions_30d=3`.
  - Make SLO stack startup render a temporary benchmark Compose file from the checked-in template with the selected worker env values instead of relying on hardcoded worker settings in the template.
  - Keep `host.json` host-only. Publish a dedicated additive `run_config` block in every per-scenario JSON artifact and in the top-level `summary.json`. Mirror the same config in the Markdown outputs so every published run is self-describing.
  - Do not change existing artifact schemas incompatibly; add fields only.

- Staged session-side tuning sweep:
  - Initial sweep varies only session-side knobs:
    - `session_batch_size`: `1000`, `2000`, `5000`
    - `session_incremental_concurrency`: `8`, `12`, `16`
    - keep `session_repair_concurrency=2`
    - keep `event_batch_size=5000`
  - Stage 1 screening:
    - run only `append-30d` and `backfill-30d`
    - force `--repetitions-30d 1`
    - discard any candidate that misses current budgets
    - compute a normalized screening score versus `artifacts/performance/2026-03-13-dim4-impact-full-r2/summary.json`
    - keep candidates with non-negative improvement on both scenarios; if more than 2 qualify, keep the top 2 by normalized score; if fewer than 2 qualify, keep the top 2 budget-passing candidates by normalized score
  - Stage 2 finalist large-window proof:
    - run only `append-90d` and `backfill-90d` for the 2 finalists
    - choose a tuning-only winner only if it improves `derived_metrics_ready_ms` by at least `10%` on `append-30d`, `backfill-30d`, and at least one of `append-90d` or `backfill-90d`
    - do not accept a winner that regresses any of those four scenarios by more than `10%` or `1000ms` absolute
    - if both finalists pass the gate, choose the one with the higher average normalized improvement across those four scenarios
  - Stage 3 repair confirmation:
    - run `repair-30d` and `repair-90d` on the provisional winner
    - keep `session_repair_concurrency=2` unless repair becomes the only remaining outlier
    - only if repair is the scenario that now blocks acceptance, run one narrow follow-up on `session_repair_concurrency={2,4}` for the winning append/backfill tuple
  - Keep `event_batch_size` fixed for this slice. Do not add it to the sweep unless a separate follow-up is explicitly requested later.

- Evidence-gated structural fallback:
  - Only enter this stage if the session-side tuning stop rule above is not met.
  - Add additive session-lane phase telemetry to freshness scenario artifacts:
    - coordinator load/group time
    - child-work time
    - rebuild-finalize time
    - grouped installs, incremental work items, repair work items, and child transactions executed
  - Choose exactly one structural optimization only if the same phase is clearly dominant across the finalist runs:
    - the same phase must be the largest measured component in `append-30d`, `backfill-30d`, and at least one of `append-90d` or `backfill-90d`
    - if no single phase is clearly dominant by that rule, stop at tuning and publish the best tuning result; do not guess at structural work
  - Structural choices:
    - if child-work time is dominant, reduce incremental child-transaction churn by processing multiple incremental install items per child transaction
    - if rebuild-finalize time is dominant, optimize pending-bucket finalization and rebuild batching first
    - if coordinator time is dominant, optimize grouped preload/state/session loading first
  - Keep repair on the current one-install-per-child-transaction shape unless the repair confirmation stage is what fails. Do not pre-commit to repair queue chunking in this slice.
  - After the single structural change lands, rerun the same staged tuning flow and then promote the final winning defaults.

- Default promotion and docs:
  - Promote the winning tuple into `WorkerConfig` defaults and both repo Compose defaults.
  - Update `docs/performance.md` with the baseline artifact used for comparison, the final promoted settings, and the fresh post-pass publication.
  - Update `docs/STATUS.md` at start and finish with whether the result came from tuning only or tuning plus one structural optimization.

## Test Plan

- Benchmark and config tests:
  - Add tests proving worker-config CLI overrides render into the temporary Compose file correctly.
  - Add tests proving the additive `run_config` block appears in every per-scenario JSON artifact and in top-level `summary.json`, while `host.json` remains host-only.
  - Add tests proving `--repetitions-30d` overrides the default `30d` repetition count without changing default behavior when omitted.

- Default-promotion proof:
  - Add tests proving the winning tuple flows through `WorkerConfig`, `required_db_connections()`, and both Compose defaults in:
    - `crates/fantasma-worker/src/scheduler.rs`
    - `infra/docker/compose.yaml`
    - `infra/docker/compose.bench.yaml`

- Worker correctness tests:
  - Expand the existing replay, failure, and repair regressions instead of building a separate proof harness.
  - If the chosen structural change is incremental child-transaction batching, extend the current replay and coordinator-failure tests to prove committed child work remains replay-safe and the offset still advances only after coordinator finalization.
  - If the chosen structural change is rebuild-finalize optimization, extend the touched-day and touched-bucket repair regressions to prove exact rebuild scope stays unchanged.
  - If the chosen structural change is coordinator preload optimization, extend the current failure-path tests to prove missing-tail and missing-daily hard failures still stop offset advance.

- Performance acceptance:
  - Final validation is the full SLO suite with the promoted defaults restored: all `append-*`, `backfill-*`, `repair-*`, and `reads-*` scenarios green.
  - Acceptance requires:
    - `derived_metrics_ready_ms` improves by at least `10%` on `append-30d`, `backfill-30d`, and at least one of `append-90d` or `backfill-90d` versus `artifacts/performance/2026-03-13-dim4-impact-full-r2/summary.json`
    - `repair-30d` and `repair-90d` stay within budget and do not regress by more than `10%` or `1000ms`
    - grouped read budgets remain green and do not materially regress as a side effect

## Assumptions

- Reads are verification only in this slice, not an optimization target.
- The first pass varies only session-side throughput knobs; `event_batch_size` stays fixed.
- This slice may add additive benchmark metadata and telemetry, but it does not change public APIs or the derived-metrics query contract.
- The comparison baseline for success is the current dim4 full-suite publication, not the older pre-dim4 append-only artifacts.
