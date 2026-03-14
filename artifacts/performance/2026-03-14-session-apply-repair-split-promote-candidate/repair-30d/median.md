# Fantasma Derived Metrics SLO: repair-30d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2

- seed_ingest: 900000 events in 30637ms (29375.92 events/s)
- repair_ingest: 3000 events in 122ms (24500.00 events/s)
- event_metrics_ready_ms: 841ms
- session_metrics_ready_ms: 101151ms
- derived_metrics_ready_ms: 101151ms
- Budget: FAIL
  - session_metrics_ready_ms readiness 101151ms exceeded budget 60000ms
  - derived_metrics_ready_ms readiness 101151ms exceeded budget 60000ms