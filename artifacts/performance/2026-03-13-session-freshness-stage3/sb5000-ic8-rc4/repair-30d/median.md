# Fantasma Derived Metrics SLO: repair-30d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 4

- seed_ingest: 900000 events in 32140ms (28002.25 events/s)
- repair_ingest: 3000 events in 121ms (24611.35 events/s)
- event_metrics_ready_ms: 978ms
- session_metrics_ready_ms: 1881ms
- derived_metrics_ready_ms: 1881ms
- Budget: PASS