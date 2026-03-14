# Fantasma Derived Metrics SLO: repair-90d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 4

- seed_ingest: 2700000 events in 92286ms (29256.64 events/s)
- repair_ingest: 9000 events in 363ms (24765.33 events/s)
- event_metrics_ready_ms: 2709ms
- session_metrics_ready_ms: 5030ms
- derived_metrics_ready_ms: 5030ms
- Budget: PASS