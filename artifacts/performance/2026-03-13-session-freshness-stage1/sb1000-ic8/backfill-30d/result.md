# Fantasma Derived Metrics SLO: backfill-30d

- Run config:
  - repetitions_30d: 1
  - worker_session_batch_size: 1000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2

- ingest: 900000 events in 29203ms (30818.74 events/s)
- event_metrics_ready_ms: 236ms
- session_metrics_ready_ms: 54236ms
- derived_metrics_ready_ms: 54236ms
- Budget: PASS