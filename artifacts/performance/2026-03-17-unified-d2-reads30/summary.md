# Fantasma Derived Metrics SLO Suite

- Environment: local
- Published suites: reads-visibility

- Run config:
  - mode: iterative
  - publication_repetitions: 1
  - worker_session_batch_size: 1000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2
  - worker_idle_poll_interval_ms: 250

## reads-visibility-30d

- Run config:
  - mode: iterative
  - publication_repetitions: 1
  - worker_session_batch_size: 1000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2
  - worker_idle_poll_interval_ms: 250

- Failure: scenario execution failed: GET http://127.0.0.1:18082/v1/metrics/sessions?metric=count&granularity=day&start=2026-01-01&end=2026-01-30&group_by=platform&group_by=app_version returned 500 Internal Server Error: {"error":"internal_server_error"}
- Budget: FAIL
  - scenario execution failed: GET http://127.0.0.1:18082/v1/metrics/sessions?metric=count&granularity=day&start=2026-01-01&end=2026-01-30&group_by=platform&group_by=app_version returned 500 Internal Server Error: {"error":"internal_server_error"}