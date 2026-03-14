# Fantasma Derived Metrics SLO: append-180d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 4

- ingest: 5400000 events in 182538ms (29582.74 events/s)
- event_metrics_ready_ms: 621ms
- session_metrics_ready_ms: 64297ms
- derived_metrics_ready_ms: 64297ms
- Budget: PASS