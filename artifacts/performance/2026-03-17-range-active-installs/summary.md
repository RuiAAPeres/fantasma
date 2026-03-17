# Fantasma Derived Metrics SLO Suite

- Environment: local
- Published suites: representative, mixed-repair, reads-visibility

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
  - worker_session_batch_size: 1000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2
  - worker_idle_poll_interval_ms: 250

- append_uploads: 225000 events in 173334ms (1298.07 events/s)
- event_metrics_ready_ms: 4ms
- session_metrics_ready_ms: 10ms
- derived_metrics_ready_ms: 10ms
- checkpoint samples: 225
- checkpoint event p95: 215ms
- checkpoint session p95: 839ms
- checkpoint derived p95: 839ms
- Budget: PASS

## live-append-plus-light-repair

- Run config:
  - mode: iterative
  - publication_repetitions: 1
  - worker_session_batch_size: 1000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2
  - worker_idle_poll_interval_ms: 250

- append_uploads: 225000 events in 129379ms (1739.06 events/s)
- repair_uploads: 180 events in 27194ms (6.62 events/s)
- event_metrics_ready_ms: 3ms
- session_metrics_ready_ms: 8ms
- derived_metrics_ready_ms: 8ms
- checkpoint samples: 234
- checkpoint event p95: 217ms
- checkpoint session p95: 1069ms
- checkpoint derived p95: 1069ms
- Budget: PASS

## reads-visibility-30d

- Run config:
  - mode: iterative
  - publication_repetitions: 1
  - worker_session_batch_size: 1000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2
  - worker_idle_poll_interval_ms: 250

- seed_ingest: 900000 events in 30218ms (29783.53 events/s)
- event_metrics_ready_ms: 228ms
- session_metrics_ready_ms: 254472ms
- derived_metrics_ready_ms: 254472ms
- Budget: PASS

| Query | p50 (ms) | p95 (ms) | min (ms) | max (ms) |
| --- | ---: | ---: | ---: | ---: |
| events_count_day_grouped | 3 | 3 | 2 | 3 |
| events_count_hour_grouped | 21 | 22 | 20 | 22 |
| events_count_day_dim2_grouped | 1 | 1 | 1 | 1 |
| events_count_hour_dim2_grouped | 7 | 7 | 6 | 8 |
| sessions_count_day_grouped | 2 | 2 | 2 | 3 |
| sessions_count_hour_grouped | 14 | 14 | 13 | 15 |
| sessions_count_day_dim2_grouped | 1 | 1 | 1 | 1 |
| sessions_count_hour_dim2_grouped | 7 | 8 | 7 | 8 |
| sessions_duration_total_day_grouped | 2 | 2 | 1 | 3 |
| sessions_duration_total_hour_grouped | 14 | 14 | 13 | 15 |
| sessions_duration_total_day_dim2_grouped | 1 | 1 | 1 | 1 |
| sessions_duration_total_hour_dim2_grouped | 7 | 8 | 7 | 8 |
| sessions_new_installs_day_grouped | 2 | 2 | 2 | 2 |
| sessions_new_installs_hour_grouped | 14 | 14 | 13 | 14 |
| sessions_new_installs_day_dim2_grouped | 1 | 1 | 1 | 1 |
| sessions_new_installs_hour_dim2_grouped | 7 | 8 | 7 | 8 |
| events_count_day_ungrouped | 0 | 0 | 0 | 0 |
| events_count_hour_ungrouped | 2 | 2 | 2 | 2 |
| sessions_count_day_ungrouped | 0 | 0 | 0 | 0 |
| sessions_count_hour_ungrouped | 2 | 2 | 2 | 2 |
| sessions_duration_total_day_ungrouped | 0 | 0 | 0 | 0 |
| sessions_duration_total_hour_ungrouped | 2 | 2 | 2 | 2 |
| sessions_new_installs_day_ungrouped | 0 | 1 | 0 | 2 |
| sessions_active_installs_day_ungrouped | 4 | 5 | 3 | 10 |
| sessions_active_installs_day_dim2_grouped | 6 | 7 | 5 | 8 |
| sessions_active_installs_day_dim2_filtered | 5 | 5 | 4 | 6 |
| sessions_new_installs_hour_ungrouped | 2 | 2 | 2 | 3 |