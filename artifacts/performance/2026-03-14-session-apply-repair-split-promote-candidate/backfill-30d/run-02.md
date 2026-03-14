# Fantasma Derived Metrics SLO: backfill-30d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2

- ingest: 900000 events in 45607ms (19733.44 events/s)
- event_metrics_ready_ms: 128ms
- session_metrics_ready_ms: 116952ms
- derived_metrics_ready_ms: 116952ms
- Budget: FAIL
  - session_metrics_ready_ms readiness 116952ms exceeded budget 60000ms
  - derived_metrics_ready_ms readiness 116952ms exceeded budget 60000ms