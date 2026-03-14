# Fantasma Derived Metrics SLO: repair-90d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2

- seed_ingest: 2700000 events in 131247ms (20571.89 events/s)
- repair_ingest: 9000 events in 444ms (20265.40 events/s)
- event_metrics_ready_ms: 1988ms
- session_metrics_ready_ms: 9678ms
- derived_metrics_ready_ms: 9678ms
- Budget: PASS