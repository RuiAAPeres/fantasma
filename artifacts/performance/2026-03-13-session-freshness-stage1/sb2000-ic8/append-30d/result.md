# Fantasma Derived Metrics SLO: append-30d

- Run config:
  - repetitions_30d: 1
  - worker_session_batch_size: 2000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2

- ingest: 900000 events in 28796ms (31254.30 events/s)
- event_metrics_ready_ms: 234ms
- session_metrics_ready_ms: 16341ms
- derived_metrics_ready_ms: 16341ms
- Budget: PASS