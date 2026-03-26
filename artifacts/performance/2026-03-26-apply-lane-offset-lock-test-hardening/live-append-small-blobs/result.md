# Fantasma Derived Metrics SLO: live-append-small-blobs

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