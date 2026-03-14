# Fantasma Derived Metrics SLO: append-90d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 16
  - worker_session_repair_concurrency: 2

- ingest: 2700000 events in 98849ms (27314.33 events/s)
- event_metrics_ready_ms: 497ms
- session_metrics_ready_ms: 30148ms
- derived_metrics_ready_ms: 30148ms
- Budget: PASS