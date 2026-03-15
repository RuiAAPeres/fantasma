# Fantasma Derived Metrics SLO Suite

- Environment: local

- Run config:
  - repetitions_30d: 1
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2

## append-30d

- Run config:
  - repetitions_30d: 1
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2

- ingest: 900000 events in 28455ms (31628.78 events/s)
- event_metrics_ready_ms: 348ms
- session_metrics_ready_ms: 10814ms
- derived_metrics_ready_ms: 10814ms
- Budget: PASS

## backfill-30d

- Run config:
  - repetitions_30d: 1
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2

- ingest: 900000 events in 29862ms (30138.04 events/s)
- event_metrics_ready_ms: 244ms
- session_metrics_ready_ms: 16079ms
- derived_metrics_ready_ms: 16079ms
- Budget: PASS