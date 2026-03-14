# Fantasma Derived Metrics SLO: backfill-30d

- Run config:
  - repetitions_30d: 1
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 12
  - worker_session_repair_concurrency: 2

- ingest: 900000 events in 32341ms (27828.05 events/s)
- event_metrics_ready_ms: 265ms
- session_metrics_ready_ms: 21396ms
- derived_metrics_ready_ms: 21396ms
- Budget: PASS