# Fantasma Derived Metrics SLO Suite

- Environment: local
- Published suites: representative, mixed-repair

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
- representative append verdict: flat
- representative checkpoint derived verdict: flat
- representative final derived verdict: flat
- mixed repair verdict: flat
- mixed repair checkpoint derived verdict: flat
- mixed repair final derived verdict: flat
- dominant Stage B phase: unchanged
- overall verdict: flat

## live-append-small-blobs

- Run config:
  - mode: iterative
  - publication_repetitions: 1
  - worker_session_batch_size: 2000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2
  - worker_idle_poll_interval_ms: 250

- append_uploads: 225000 events in 105504ms (2132.61 events/s)
- event_metrics_ready_ms: 2ms
- session_metrics_ready_ms: 6ms
- derived_metrics_ready_ms: 6ms
- checkpoint samples: 225
- checkpoint event p95: 107ms
- checkpoint session p95: 214ms
- checkpoint derived p95: 214ms
- Budget: PASS

## live-append-plus-light-repair

- Run config:
  - mode: iterative
  - publication_repetitions: 1
  - worker_session_batch_size: 2000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2
  - worker_idle_poll_interval_ms: 250

- append_uploads: 225000 events in 118562ms (1897.74 events/s)
- repair_uploads: 180 events in 6511ms (27.64 events/s)
- event_metrics_ready_ms: 1ms
- session_metrics_ready_ms: 5ms
- derived_metrics_ready_ms: 5ms
- checkpoint samples: 234
- checkpoint event p95: 110ms
- checkpoint session p95: 318ms
- checkpoint derived p95: 318ms
- Budget: PASS