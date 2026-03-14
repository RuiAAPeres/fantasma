# Fantasma Derived Metrics SLO: append-30d

- Run config:
  - repetitions_30d: 1
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 12
  - worker_session_repair_concurrency: 2

- ingest: 900000 events in 28371ms (31721.62 events/s)
- event_metrics_ready_ms: 243ms
- session_metrics_ready_ms: 11336ms
- derived_metrics_ready_ms: 11336ms
- Budget: PASS