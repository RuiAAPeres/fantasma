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
  - enable_session_daily_deferred_rebuilds: false
  - enable_session_metric_deferred_rebuilds: true
  - enable_install_activity_deferred_drains: true

## Iterative Verdict

- baseline: docs/STATUS.md benchmark usability reset entry (2026-03-15 representative + mixed-repair iterative smokes)
- representative append verdict: flat
- representative checkpoint derived verdict: flat
- representative final derived verdict: flat
- overall verdict: flat

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
  - enable_session_daily_deferred_rebuilds: false
  - enable_session_metric_deferred_rebuilds: true
  - enable_install_activity_deferred_drains: true

- append_uploads: 225000 events in 257699ms (873.11 events/s)
- event_metrics_ready_ms: 1ms
- session_metrics_ready_ms: 5ms
- derived_metrics_ready_ms: 5ms
- checkpoint samples: 225
- checkpoint event p95: 833ms
- checkpoint session p95: 836ms
- checkpoint derived p95: 942ms
- Budget: PASS