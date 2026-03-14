# Fantasma Derived Metrics SLO: backfill-30d

- Run config:
  - repetitions_30d: 1
  - worker_session_batch_size: 1000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 16
  - worker_session_repair_concurrency: 2

- ingest: 900000 events in 32283ms (27878.21 events/s)
- event_metrics_ready_ms: 267ms
- session_metrics_ready_ms: 51825ms
- derived_metrics_ready_ms: 51825ms
- Budget: PASS