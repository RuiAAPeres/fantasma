# Fantasma Derived Metrics SLO: repair-30d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 4

- seed_ingest: 900000 events in 31562ms (28515.04 events/s)
- repair_ingest: 3000 events in 102ms (29136.70 events/s)
- event_metrics_ready_ms: 1104ms
- session_metrics_ready_ms: 1926ms
- derived_metrics_ready_ms: 1926ms
- Budget: PASS