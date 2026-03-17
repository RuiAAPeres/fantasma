# Fantasma Derived Metrics SLO: live-append-plus-light-repair

- Run config:
  - mode: iterative
  - publication_repetitions: 1
  - worker_session_batch_size: 1000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2
  - worker_idle_poll_interval_ms: 250

- append_uploads: 225000 events in 129379ms (1739.06 events/s)
- repair_uploads: 180 events in 27194ms (6.62 events/s)
- event_metrics_ready_ms: 3ms
- session_metrics_ready_ms: 8ms
- derived_metrics_ready_ms: 8ms
- checkpoint samples: 234
- checkpoint event p95: 217ms
- checkpoint session p95: 1069ms
- checkpoint derived p95: 1069ms
- Budget: PASS