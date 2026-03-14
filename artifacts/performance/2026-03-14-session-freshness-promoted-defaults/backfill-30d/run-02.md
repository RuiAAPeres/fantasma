# Fantasma Derived Metrics SLO: backfill-30d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 4

- ingest: 900000 events in 31894ms (28218.08 events/s)
- event_metrics_ready_ms: 254ms
- session_metrics_ready_ms: 10566ms
- derived_metrics_ready_ms: 10566ms
- Budget: PASS