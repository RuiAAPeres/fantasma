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
  - enable_session_projection_deferred_drains: false
  - enable_install_activity_deferred_drains: true

- Failure: scenario execution failed: timed out waiting for checkpoint readiness (session metrics)
- Budget: FAIL
  - scenario execution failed: timed out waiting for checkpoint readiness (session metrics)