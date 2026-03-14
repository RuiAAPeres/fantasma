# Fantasma Derived Metrics SLO: backfill-30d

- Run config:
  - repetitions_30d: 1
  - worker_session_batch_size: 2000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2

- ingest: 900000 events in 28277ms (31827.41 events/s)
- event_metrics_ready_ms: 347ms
- session_metrics_ready_ms: 33632ms
- derived_metrics_ready_ms: 33632ms
- Budget: PASS