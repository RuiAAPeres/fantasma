# Fantasma Derived Metrics SLO: live-append-plus-light-repair

- Run config:
  - mode: iterative
  - publication_repetitions: 1
  - worker_session_batch_size: 2000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2
  - worker_idle_poll_interval_ms: 250

- append_uploads: 225000 events in 118562ms (1897.74 events/s)
- repair_uploads: 180 events in 6511ms (27.64 events/s)
- event_metrics_ready_ms: 1ms
- session_metrics_ready_ms: 5ms
- derived_metrics_ready_ms: 5ms
- checkpoint samples: 234
- checkpoint event p95: 110ms
- checkpoint session p95: 318ms
- checkpoint derived p95: 318ms
- Budget: PASS