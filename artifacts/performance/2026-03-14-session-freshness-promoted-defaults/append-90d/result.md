# Fantasma Derived Metrics SLO: append-90d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 4

- ingest: 2700000 events in 90937ms (29690.68 events/s)
- event_metrics_ready_ms: 382ms
- session_metrics_ready_ms: 34335ms
- derived_metrics_ready_ms: 34335ms
- Budget: PASS