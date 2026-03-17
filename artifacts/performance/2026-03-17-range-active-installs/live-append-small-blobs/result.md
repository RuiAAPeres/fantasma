# Fantasma Derived Metrics SLO: live-append-small-blobs

- Run config:
  - mode: iterative
  - publication_repetitions: 1
  - worker_session_batch_size: 1000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2
  - worker_idle_poll_interval_ms: 250

- append_uploads: 225000 events in 173334ms (1298.07 events/s)
- event_metrics_ready_ms: 4ms
- session_metrics_ready_ms: 10ms
- derived_metrics_ready_ms: 10ms
- checkpoint samples: 225
- checkpoint event p95: 215ms
- checkpoint session p95: 839ms
- checkpoint derived p95: 839ms
- Budget: PASS