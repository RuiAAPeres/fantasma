# Fantasma Derived Metrics SLO: repair-30d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 4

- seed_ingest: 900000 events in 29347ms (30666.51 events/s)
- repair_ingest: 3000 events in 120ms (24954.14 events/s)
- event_metrics_ready_ms: 1001ms
- session_metrics_ready_ms: 1881ms
- derived_metrics_ready_ms: 1881ms
- Budget: PASS