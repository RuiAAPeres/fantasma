# Fantasma Derived Metrics SLO Suite

- Environment: local

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2

## append-90d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2

- ingest: 2700000 events in 94848ms (28466.51 events/s)
- event_metrics_ready_ms: 417ms
- session_metrics_ready_ms: 30167ms
- derived_metrics_ready_ms: 30167ms
- Budget: PASS

## backfill-90d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2

- ingest: 2700000 events in 106969ms (25240.78 events/s)
- event_metrics_ready_ms: 413ms
- session_metrics_ready_ms: 78087ms
- derived_metrics_ready_ms: 78087ms
- Budget: PASS