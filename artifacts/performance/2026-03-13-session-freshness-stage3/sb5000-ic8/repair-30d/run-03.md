# Fantasma Derived Metrics SLO: repair-30d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2

- seed_ingest: 900000 events in 31457ms (28610.24 events/s)
- repair_ingest: 3000 events in 119ms (25193.71 events/s)
- event_metrics_ready_ms: 1141ms
- session_metrics_ready_ms: 3376ms
- derived_metrics_ready_ms: 3376ms
- Budget: PASS