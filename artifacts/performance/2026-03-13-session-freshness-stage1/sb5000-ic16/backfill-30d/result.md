# Fantasma Derived Metrics SLO: backfill-30d

- Run config:
  - repetitions_30d: 1
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 16
  - worker_session_repair_concurrency: 2

- ingest: 900000 events in 31682ms (28407.00 events/s)
- event_metrics_ready_ms: 355ms
- session_metrics_ready_ms: 16800ms
- derived_metrics_ready_ms: 16800ms
- Budget: PASS