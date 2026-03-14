# Fantasma Derived Metrics SLO: backfill-30d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2

- ingest: 900000 events in 32046ms (28084.25 events/s)
- event_metrics_ready_ms: 343ms
- session_metrics_ready_ms: 113217ms
- derived_metrics_ready_ms: 113217ms
- Budget: FAIL
  - session_metrics_ready_ms readiness 113217ms exceeded budget 60000ms
  - derived_metrics_ready_ms readiness 113217ms exceeded budget 60000ms