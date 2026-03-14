# Fantasma Derived Metrics SLO: append-30d

- Run config:
  - repetitions_30d: 1
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 16
  - worker_session_repair_concurrency: 2

- ingest: 900000 events in 32586ms (27618.73 events/s)
- event_metrics_ready_ms: 244ms
- session_metrics_ready_ms: 10742ms
- derived_metrics_ready_ms: 10742ms
- Budget: PASS