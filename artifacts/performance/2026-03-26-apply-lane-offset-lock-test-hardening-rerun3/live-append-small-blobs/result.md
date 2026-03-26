# Fantasma Derived Metrics SLO: live-append-small-blobs

- Run config:
  - mode: iterative
  - publication_repetitions: 1
  - worker_session_batch_size: 2000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2
  - worker_idle_poll_interval_ms: 250

- append_uploads: 225000 events in 106229ms (2118.06 events/s)
- event_metrics_ready_ms: 0ms
- session_metrics_ready_ms: 2ms
- derived_metrics_ready_ms: 2ms
- checkpoint samples: 225
- checkpoint event p95: 108ms
- checkpoint session p95: 314ms
- checkpoint derived p95: 314ms
- Budget: PASS