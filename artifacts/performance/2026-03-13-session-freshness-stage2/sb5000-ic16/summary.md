# Fantasma Derived Metrics SLO Suite

- Host: Apple M3 Pro / 36 GiB / Darwin 25.1.0 / aarch64
- Benchmarked at: 2026-03-13T23:56:54.810327+00:00

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 16
  - worker_session_repair_concurrency: 2

## append-90d

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

## backfill-90d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 16
  - worker_session_repair_concurrency: 2

- ingest: 2700000 events in 98236ms (27484.60 events/s)
- event_metrics_ready_ms: 488ms
- session_metrics_ready_ms: 76110ms
- derived_metrics_ready_ms: 76110ms
- Budget: PASS