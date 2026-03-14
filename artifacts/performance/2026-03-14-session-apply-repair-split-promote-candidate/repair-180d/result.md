# Fantasma Derived Metrics SLO: repair-180d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2

- seed_ingest: 5400000 events in 287250ms (18798.94 events/s)
- repair_ingest: 18000 events in 859ms (20936.57 events/s)
- event_metrics_ready_ms: 6384ms
- session_metrics_ready_ms: 1331810ms
- derived_metrics_ready_ms: 1331810ms
- Budget: PASS