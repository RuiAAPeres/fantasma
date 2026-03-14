# Fantasma Derived Metrics SLO: repair-90d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2

- seed_ingest: 2700000 events in 93675ms (28822.98 events/s)
- repair_ingest: 9000 events in 338ms (26583.19 events/s)
- event_metrics_ready_ms: 2708ms
- session_metrics_ready_ms: 8107ms
- derived_metrics_ready_ms: 8107ms
- Budget: PASS