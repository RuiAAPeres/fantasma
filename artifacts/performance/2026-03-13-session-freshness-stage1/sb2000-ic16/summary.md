# Fantasma Derived Metrics SLO Suite

- Host: Apple M3 Pro / 36 GiB / Darwin 25.1.0 / aarch64
- Benchmarked at: 2026-03-13T23:44:00.083907+00:00

- Run config:
  - repetitions_30d: 1
  - worker_session_batch_size: 2000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 16
  - worker_session_repair_concurrency: 2

## append-30d

- Run config:
  - repetitions_30d: 1
  - worker_session_batch_size: 2000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 16
  - worker_session_repair_concurrency: 2

- ingest: 900000 events in 29369ms (30643.54 events/s)
- event_metrics_ready_ms: 271ms
- session_metrics_ready_ms: 18939ms
- derived_metrics_ready_ms: 18939ms
- Budget: PASS

## backfill-30d

- Run config:
  - repetitions_30d: 1
  - worker_session_batch_size: 2000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 16
  - worker_session_repair_concurrency: 2

- ingest: 900000 events in 31757ms (28339.92 events/s)
- event_metrics_ready_ms: 234ms
- session_metrics_ready_ms: 27551ms
- derived_metrics_ready_ms: 27551ms
- Budget: PASS