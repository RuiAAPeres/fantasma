# Fantasma Derived Metrics SLO: append-30d

- Run config:
  - repetitions_30d: 1
  - worker_session_batch_size: 1000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2

- ingest: 900000 events in 30833ms (29188.65 events/s)
- event_metrics_ready_ms: 347ms
- session_metrics_ready_ms: 23876ms
- derived_metrics_ready_ms: 23876ms
- Budget: PASS