# Fantasma Derived Metrics SLO: repair-30d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2

- seed_ingest: 900000 events in 28360ms (31734.51 events/s)
- repair_ingest: 3000 events in 120ms (24829.43 events/s)
- event_metrics_ready_ms: 993ms
- session_metrics_ready_ms: 2636ms
- derived_metrics_ready_ms: 2636ms
- Budget: PASS