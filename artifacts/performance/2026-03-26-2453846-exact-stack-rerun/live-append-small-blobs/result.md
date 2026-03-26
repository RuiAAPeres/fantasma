# Fantasma Derived Metrics SLO: live-append-small-blobs

- Run config:
  - mode: iterative
  - publication_repetitions: 1
  - worker_session_batch_size: 2000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2
  - worker_idle_poll_interval_ms: 250

- append_uploads: 225000 events in 105504ms (2132.61 events/s)
- event_metrics_ready_ms: 2ms
- session_metrics_ready_ms: 6ms
- derived_metrics_ready_ms: 6ms
- checkpoint samples: 225
- checkpoint event p95: 107ms
- checkpoint session p95: 214ms
- checkpoint derived p95: 214ms
- Budget: PASS