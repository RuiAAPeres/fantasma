# Fantasma Derived Metrics SLO: append-30d

- Run config:
  - repetitions_30d: 1
  - worker_session_batch_size: 2000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 12
  - worker_session_repair_concurrency: 2

- ingest: 900000 events in 35809ms (25133.27 events/s)
- event_metrics_ready_ms: 255ms
- session_metrics_ready_ms: 17434ms
- derived_metrics_ready_ms: 17434ms
- Budget: PASS