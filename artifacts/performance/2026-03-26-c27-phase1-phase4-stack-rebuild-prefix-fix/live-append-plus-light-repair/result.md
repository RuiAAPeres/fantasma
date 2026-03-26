# Fantasma Derived Metrics SLO: live-append-plus-light-repair

- Run config:
  - mode: iterative
  - publication_repetitions: 1
  - worker_session_batch_size: 2000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2
  - worker_idle_poll_interval_ms: 250

- append_uploads: 225000 events in 106898ms (2104.79 events/s)
- repair_uploads: 180 events in 7280ms (24.72 events/s)
- event_metrics_ready_ms: 0ms
- session_metrics_ready_ms: 2ms
- derived_metrics_ready_ms: 2ms
- checkpoint samples: 234
- checkpoint event p95: 107ms
- checkpoint session p95: 211ms
- checkpoint derived p95: 211ms
- Budget: PASS