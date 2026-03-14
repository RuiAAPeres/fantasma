# Fantasma Derived Metrics SLO: repair-30d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2

- seed_ingest: 900000 events in 32102ms (28035.45 events/s)
- repair_ingest: 3000 events in 130ms (23068.31 events/s)
- event_metrics_ready_ms: 872ms
- session_metrics_ready_ms: 1742ms
- derived_metrics_ready_ms: 1742ms
- Budget: PASS