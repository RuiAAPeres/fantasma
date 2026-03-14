# Fantasma Derived Metrics SLO: repair-30d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2

- seed_ingest: 900000 events in 30642ms (29371.31 events/s)
- repair_ingest: 3000 events in 122ms (24549.88 events/s)
- event_metrics_ready_ms: 842ms
- session_metrics_ready_ms: 109948ms
- derived_metrics_ready_ms: 109948ms
- Budget: FAIL
  - session_metrics_ready_ms readiness 109948ms exceeded budget 60000ms
  - derived_metrics_ready_ms readiness 109948ms exceeded budget 60000ms