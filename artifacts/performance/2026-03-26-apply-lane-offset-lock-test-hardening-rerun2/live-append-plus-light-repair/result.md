# Fantasma Derived Metrics SLO: live-append-plus-light-repair

- Run config:
  - mode: iterative
  - publication_repetitions: 1
  - worker_session_batch_size: 2000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2
  - worker_idle_poll_interval_ms: 250

- append_uploads: 225000 events in 111146ms (2024.36 events/s)
- repair_uploads: 180 events in 7010ms (25.68 events/s)
- event_metrics_ready_ms: 1ms
- session_metrics_ready_ms: 3ms
- derived_metrics_ready_ms: 3ms
- checkpoint samples: 234
- checkpoint event p95: 107ms
- checkpoint session p95: 318ms
- checkpoint derived p95: 318ms
- Budget: PASS