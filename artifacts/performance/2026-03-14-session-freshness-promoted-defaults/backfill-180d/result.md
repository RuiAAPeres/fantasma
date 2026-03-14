# Fantasma Derived Metrics SLO: backfill-180d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 4

- ingest: 5400000 events in 185368ms (29131.13 events/s)
- event_metrics_ready_ms: 621ms
- session_metrics_ready_ms: 142979ms
- derived_metrics_ready_ms: 142979ms
- Budget: PASS