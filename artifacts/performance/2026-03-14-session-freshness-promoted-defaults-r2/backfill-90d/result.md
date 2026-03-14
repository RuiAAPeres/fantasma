# Fantasma Derived Metrics SLO: backfill-90d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2

- ingest: 2700000 events in 100465ms (26874.84 events/s)
- event_metrics_ready_ms: 256ms
- session_metrics_ready_ms: 67788ms
- derived_metrics_ready_ms: 67788ms
- Budget: PASS