# Fantasma Derived Metrics SLO: repair-180d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 4

- seed_ingest: 5400000 events in 275589ms (19594.39 events/s)
- repair_ingest: 18000 events in 1626ms (11065.31 events/s)
- event_metrics_ready_ms: 4822ms
- session_metrics_ready_ms: 15603ms
- derived_metrics_ready_ms: 15603ms
- Budget: PASS