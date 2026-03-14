# Fantasma Derived Metrics SLO: append-30d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2

- ingest: 900000 events in 29116ms (30910.53 events/s)
- event_metrics_ready_ms: 239ms
- session_metrics_ready_ms: 11176ms
- derived_metrics_ready_ms: 11176ms
- Budget: PASS