# Fantasma Derived Metrics SLO Suite

- Environment: local
- Published suites: representative

- Run config:
  - mode: iterative
  - publication_repetitions: 1
  - worker_session_batch_size: 2000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2
  - worker_idle_poll_interval_ms: 250
  - enable_event_metric_deferred_drains: true
  - enable_session_daily_deferred_rebuilds: true
  - enable_session_metric_deferred_rebuilds: false
  - enable_install_activity_deferred_drains: true

## Iterative Verdict

- baseline: docs/STATUS.md benchmark usability reset entry (2026-03-15 representative + mixed-repair iterative smokes)
- representative append verdict: worse
- representative checkpoint sample count: mismatch vs baseline
- representative final derived verdict: worse
- overall verdict: worse

## live-append-small-blobs

- Run config:
  - mode: iterative
  - publication_repetitions: 1
  - worker_session_batch_size: 2000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2
  - worker_idle_poll_interval_ms: 250
  - enable_event_metric_deferred_drains: true
  - enable_session_daily_deferred_rebuilds: true
  - enable_session_metric_deferred_rebuilds: false
  - enable_install_activity_deferred_drains: true

- Failure: scenario execution failed: timed out waiting for checkpoint readiness (session metrics)
- Budget: FAIL
  - scenario execution failed: timed out waiting for checkpoint readiness (session metrics)