# Fantasma Derived Metrics SLO Suite

- Host: Apple M3 Pro / 36 GiB / Darwin 25.1.0 / aarch64
- Benchmarked at: 2026-03-14T00:07:58.587629+00:00

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 4

## repair-30d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 4

- seed_ingest: 900000 events in 32140ms (28002.25 events/s)
- repair_ingest: 3000 events in 121ms (24611.35 events/s)
- event_metrics_ready_ms: 978ms
- session_metrics_ready_ms: 1881ms
- derived_metrics_ready_ms: 1881ms
- Budget: PASS

## repair-90d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 4

- seed_ingest: 2700000 events in 92286ms (29256.64 events/s)
- repair_ingest: 9000 events in 363ms (24765.33 events/s)
- event_metrics_ready_ms: 2709ms
- session_metrics_ready_ms: 5030ms
- derived_metrics_ready_ms: 5030ms
- Budget: PASS