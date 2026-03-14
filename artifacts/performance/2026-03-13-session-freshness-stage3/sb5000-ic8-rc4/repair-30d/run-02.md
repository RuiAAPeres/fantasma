# Fantasma Derived Metrics SLO: repair-30d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 4

- seed_ingest: 900000 events in 37673ms (23889.69 events/s)
- repair_ingest: 3000 events in 149ms (20098.29 events/s)
- event_metrics_ready_ms: 978ms
- session_metrics_ready_ms: 2399ms
- derived_metrics_ready_ms: 2399ms
- Budget: PASS