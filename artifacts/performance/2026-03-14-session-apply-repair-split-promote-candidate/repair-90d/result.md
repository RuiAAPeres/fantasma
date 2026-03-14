# Fantasma Derived Metrics SLO: repair-90d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2

- seed_ingest: 2700000 events in 110800ms (24368.18 events/s)
- repair_ingest: 9000 events in 380ms (23677.17 events/s)
- event_metrics_ready_ms: 2343ms
- session_metrics_ready_ms: 568298ms
- derived_metrics_ready_ms: 568298ms
- Budget: PASS