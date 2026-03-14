# Fantasma Derived Metrics SLO Suite

- Host: Apple M3 Pro / 36 GiB / Darwin 25.1.0 / aarch64
- Benchmarked at: 2026-03-13T23:39:48.349669+00:00

- Run config:
  - repetitions_30d: 1
  - worker_session_batch_size: 2000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2

## append-30d

- Run config:
  - repetitions_30d: 1
  - worker_session_batch_size: 2000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2

- ingest: 900000 events in 28796ms (31254.30 events/s)
- event_metrics_ready_ms: 234ms
- session_metrics_ready_ms: 16341ms
- derived_metrics_ready_ms: 16341ms
- Budget: PASS

## backfill-30d

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