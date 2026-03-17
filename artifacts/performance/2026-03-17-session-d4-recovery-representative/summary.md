# Fantasma Derived Metrics SLO Suite

- Environment: local
- Published suites: representative

- Run config:
  - mode: iterative
  - publication_repetitions: 1
  - worker_session_batch_size: 1000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2
  - worker_idle_poll_interval_ms: 250

## Iterative Verdict

- baseline: docs/STATUS.md benchmark usability reset entry (2026-03-15 representative + mixed-repair iterative smokes)
- representative append verdict: worse
- representative checkpoint derived verdict: worse
- representative final derived verdict: flat
- overall verdict: worse

## live-append-small-blobs

- Run config:
  - mode: iterative
  - publication_repetitions: 1
  - worker_session_batch_size: 1000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2
  - worker_idle_poll_interval_ms: 250

- append_uploads: 225000 events in 212385ms (1059.40 events/s)
- event_metrics_ready_ms: 4ms
- session_metrics_ready_ms: 9ms
- derived_metrics_ready_ms: 9ms
- checkpoint samples: 225
- checkpoint event p95: 327ms
- checkpoint session p95: 1485ms
- checkpoint derived p95: 1485ms
- Budget: PASS