# Fantasma Derived Metrics SLO Suite

- Environment: local

- Run config:
  - repetitions_30d: 1
  - worker_session_batch_size: 1000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 12
  - worker_session_repair_concurrency: 2

## append-30d

- Run config:
  - repetitions_30d: 1
  - worker_session_batch_size: 1000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 12
  - worker_session_repair_concurrency: 2

- ingest: 900000 events in 35151ms (25603.67 events/s)
- event_metrics_ready_ms: 261ms
- session_metrics_ready_ms: 28826ms
- derived_metrics_ready_ms: 28826ms
- Budget: PASS

## backfill-30d

- Run config:
  - repetitions_30d: 1
  - worker_session_batch_size: 1000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 12
  - worker_session_repair_concurrency: 2

- ingest: 900000 events in 33979ms (26486.64 events/s)
- event_metrics_ready_ms: 237ms
- session_metrics_ready_ms: 51457ms
- derived_metrics_ready_ms: 51457ms
- Budget: PASS