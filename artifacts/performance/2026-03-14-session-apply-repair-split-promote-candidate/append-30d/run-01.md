# Fantasma Derived Metrics SLO: append-30d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2

- ingest: 900000 events in 31370ms (28689.78 events/s)
- event_metrics_ready_ms: 240ms
- session_metrics_ready_ms: 9354ms
- derived_metrics_ready_ms: 9354ms
- Budget: PASS