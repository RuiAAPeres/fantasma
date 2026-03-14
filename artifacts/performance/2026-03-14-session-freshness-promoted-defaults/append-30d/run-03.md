# Fantasma Derived Metrics SLO: append-30d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 4

- ingest: 900000 events in 30198ms (29802.92 events/s)
- event_metrics_ready_ms: 357ms
- session_metrics_ready_ms: 11665ms
- derived_metrics_ready_ms: 11665ms
- Budget: PASS