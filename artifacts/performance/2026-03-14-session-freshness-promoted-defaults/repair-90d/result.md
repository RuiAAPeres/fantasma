# Fantasma Derived Metrics SLO: repair-90d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 4

- seed_ingest: 2700000 events in 85534ms (31566.13 events/s)
- repair_ingest: 9000 events in 405ms (22189.46 events/s)
- event_metrics_ready_ms: 2486ms
- session_metrics_ready_ms: 7112ms
- derived_metrics_ready_ms: 7112ms
- Budget: PASS