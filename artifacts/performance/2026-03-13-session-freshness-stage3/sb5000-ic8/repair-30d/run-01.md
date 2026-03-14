# Fantasma Derived Metrics SLO: repair-30d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2

- seed_ingest: 900000 events in 38638ms (23292.76 events/s)
- repair_ingest: 3000 events in 146ms (20520.68 events/s)
- event_metrics_ready_ms: 1006ms
- session_metrics_ready_ms: 1658ms
- derived_metrics_ready_ms: 1658ms
- Budget: PASS