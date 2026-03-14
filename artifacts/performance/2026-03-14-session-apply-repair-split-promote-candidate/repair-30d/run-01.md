# Fantasma Derived Metrics SLO: repair-30d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2

- seed_ingest: 900000 events in 30637ms (29375.92 events/s)
- repair_ingest: 3000 events in 133ms (22487.89 events/s)
- event_metrics_ready_ms: 769ms
- session_metrics_ready_ms: 99317ms
- derived_metrics_ready_ms: 99317ms
- Budget: FAIL
  - session_metrics_ready_ms readiness 99317ms exceeded budget 60000ms
  - derived_metrics_ready_ms readiness 99317ms exceeded budget 60000ms