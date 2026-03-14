# Fantasma Derived Metrics SLO: append-30d

- Run config:
  - repetitions_30d: 1
  - worker_session_batch_size: 1000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2

- ingest: 900000 events in 33050ms (27230.88 events/s)
- event_metrics_ready_ms: 157ms
- session_metrics_ready_ms: 23480ms
- derived_metrics_ready_ms: 23480ms
- Budget: PASS