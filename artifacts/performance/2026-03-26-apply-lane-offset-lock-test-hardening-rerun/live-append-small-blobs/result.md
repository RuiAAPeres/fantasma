# Fantasma Derived Metrics SLO: live-append-small-blobs

- Run config:
  - mode: iterative
  - publication_repetitions: 1
  - worker_session_batch_size: 2000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2
  - worker_idle_poll_interval_ms: 250

- Failure: scenario execution failed: GET http://127.0.0.1:18082/v1/metrics/sessions?metric=count&granularity=day&start=2026-01-01&end=2026-01-30: error sending request for url (http://127.0.0.1:18082/v1/metrics/sessions?metric=count&granularity=day&start=2026-01-01&end=2026-01-30): client error (SendRequest): connection error: Connection reset by peer (os error 54)
- Budget: FAIL
  - scenario execution failed: GET http://127.0.0.1:18082/v1/metrics/sessions?metric=count&granularity=day&start=2026-01-01&end=2026-01-30: error sending request for url (http://127.0.0.1:18082/v1/metrics/sessions?metric=count&granularity=day&start=2026-01-01&end=2026-01-30): client error (SendRequest): connection error: Connection reset by peer (os error 54)