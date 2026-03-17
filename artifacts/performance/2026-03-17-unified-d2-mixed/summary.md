# Fantasma Derived Metrics SLO Suite

- Environment: local
- Published suites: representative, mixed-repair

- Run config:
  - mode: iterative
  - publication_repetitions: 1
  - worker_session_batch_size: 1000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2
  - worker_idle_poll_interval_ms: 250

## Iterative Verdict

- baseline: docs/STATUS.md benchmark usability reset entry (2026-03-15 representative + mixed-repair iterative smokes)
- mixed repair verdict: flat
- mixed repair checkpoint derived verdict: flat
- mixed repair final derived verdict: flat
- dominant Stage B phase: unchanged
- overall verdict: flat

## live-append-plus-light-repair

- Run config:
  - mode: iterative
  - publication_repetitions: 1
  - worker_session_batch_size: 1000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2
  - worker_idle_poll_interval_ms: 250

- append_uploads: 225000 events in 101152ms (2224.37 events/s)
- repair_uploads: 180 events in 9569ms (18.81 events/s)
- event_metrics_ready_ms: 10ms
- session_metrics_ready_ms: 18ms
- derived_metrics_ready_ms: 18ms
- checkpoint samples: 234
- checkpoint event p95: 217ms
- checkpoint session p95: 632ms
- checkpoint derived p95: 632ms
- Budget: PASS