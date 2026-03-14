# Fantasma Derived Metrics SLO: append-90d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2

- ingest: 2700000 events in 98147ms (27509.58 events/s)
- event_metrics_ready_ms: 472ms
- session_metrics_ready_ms: 23716ms
- derived_metrics_ready_ms: 23716ms
- Budget: PASS