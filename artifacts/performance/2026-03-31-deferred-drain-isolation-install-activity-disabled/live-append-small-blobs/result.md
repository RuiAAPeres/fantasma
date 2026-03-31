# Fantasma Derived Metrics SLO: live-append-small-blobs

- Run config:
  - mode: iterative
  - publication_repetitions: 1
  - worker_session_batch_size: 2000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2
  - worker_idle_poll_interval_ms: 250
  - enable_event_metric_deferred_drains: true
  - enable_session_projection_deferred_drains: true
  - enable_install_activity_deferred_drains: false

- append_uploads: 225000 events in 249969ms (900.11 events/s)
- event_metrics_ready_ms: 3ms
- session_metrics_ready_ms: 7ms
- derived_metrics_ready_ms: 7ms
- checkpoint samples: 225
- checkpoint event p95: 631ms
- checkpoint session p95: 423ms
- checkpoint derived p95: 734ms
- Budget: PASS