# Fantasma Derived Metrics SLO: backfill-30d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2

- ingest: 900000 events in 31031ms (29003.04 events/s)
- event_metrics_ready_ms: 136ms
- session_metrics_ready_ms: 13108ms
- derived_metrics_ready_ms: 13108ms
- Budget: PASS