# Fantasma Derived Metrics SLO: append-180d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2

- ingest: 5400000 events in 190528ms (28342.16 events/s)
- event_metrics_ready_ms: 734ms
- session_metrics_ready_ms: 51136ms
- derived_metrics_ready_ms: 51136ms
- Budget: PASS