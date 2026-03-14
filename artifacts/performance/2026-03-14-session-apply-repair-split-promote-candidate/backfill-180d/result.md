# Fantasma Derived Metrics SLO: backfill-180d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2

- ingest: 5400000 events in 227259ms (23761.43 events/s)
- event_metrics_ready_ms: 450ms
- session_metrics_ready_ms: 657273ms
- derived_metrics_ready_ms: 657273ms
- Budget: PASS