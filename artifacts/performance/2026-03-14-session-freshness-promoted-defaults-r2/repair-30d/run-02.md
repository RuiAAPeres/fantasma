# Fantasma Derived Metrics SLO: repair-30d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2

- seed_ingest: 900000 events in 27987ms (32156.66 events/s)
- repair_ingest: 3000 events in 103ms (28936.23 events/s)
- event_metrics_ready_ms: 863ms
- session_metrics_ready_ms: 2344ms
- derived_metrics_ready_ms: 2344ms
- Budget: PASS