# Fantasma Derived Metrics SLO Suite

- Environment: local
- Published suites: representative

- Run config:
  - mode: iterative
  - publication_repetitions: 1
  - worker_session_batch_size: 2000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2
  - worker_idle_poll_interval_ms: 250

## Iterative Verdict

- baseline: docs/STATUS.md benchmark usability reset entry (2026-03-15 representative + mixed-repair iterative smokes)
- representative append verdict: worse
- representative checkpoint sample count: mismatch vs baseline
- representative final derived verdict: worse
- overall verdict: worse

## live-append-small-blobs

- Run config:
  - mode: iterative
  - publication_repetitions: 1
  - worker_session_batch_size: 2000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2
  - worker_idle_poll_interval_ms: 250

- Failure: scenario execution failed: POST /v1/events: error sending request for url (http://127.0.0.1:18081/v1/events): client error (SendRequest): connection closed before message completed
- Budget: FAIL
  - scenario execution failed: POST /v1/events: error sending request for url (http://127.0.0.1:18081/v1/events): client error (SendRequest): connection closed before message completed