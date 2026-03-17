# Fantasma Derived Metrics SLO: live-append-plus-light-repair

- Run config:
  - mode: iterative
  - publication_repetitions: 1
  - worker_session_batch_size: 1000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2
  - worker_idle_poll_interval_ms: 250

- append_uploads: 225000 events in 203518ms (1105.55 events/s)
- repair_uploads: 180 events in 42189ms (4.27 events/s)
- event_metrics_ready_ms: 7ms
- session_metrics_ready_ms: 19ms
- derived_metrics_ready_ms: 19ms
- checkpoint samples: 234
- checkpoint event p95: 329ms
- checkpoint session p95: 1784ms
- checkpoint derived p95: 1784ms
- Budget: PASS