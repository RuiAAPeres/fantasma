# Fantasma Derived Metrics SLO Suite

- Environment: local

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2

## repair-30d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2

- seed_ingest: 900000 events in 32102ms (28035.45 events/s)
- repair_ingest: 3000 events in 130ms (23068.31 events/s)
- event_metrics_ready_ms: 1006ms
- session_metrics_ready_ms: 1742ms
- derived_metrics_ready_ms: 1742ms
- Budget: PASS

## repair-90d

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