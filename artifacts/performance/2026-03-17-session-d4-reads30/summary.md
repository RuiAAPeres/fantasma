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

- seed_ingest: 900000 events in 35823ms (25123.23 events/s)
- event_metrics_ready_ms: 235ms
- session_metrics_ready_ms: 166309ms
- derived_metrics_ready_ms: 166309ms
- Budget: PASS

| Query | p50 (ms) | p95 (ms) | min (ms) | max (ms) |
| --- | ---: | ---: | ---: | ---: |
| events_count_day_grouped | 3 | 4 | 3 | 4 |
| events_count_hour_grouped | 29 | 30 | 28 | 31 |
| events_count_day_dim4_grouped | 3 | 3 | 2 | 4 |
| events_count_hour_dim4_grouped | 25 | 26 | 23 | 26 |
| sessions_count_day_grouped | 2 | 3 | 2 | 3 |
| sessions_count_hour_grouped | 15 | 16 | 14 | 18 |
| sessions_count_day_dim4_grouped | 3 | 3 | 2 | 4 |
| sessions_count_hour_dim4_grouped | 24 | 25 | 23 | 26 |
| sessions_duration_total_day_grouped | 2 | 2 | 2 | 3 |
| sessions_duration_total_hour_grouped | 15 | 16 | 15 | 18 |
| sessions_duration_total_day_dim4_grouped | 3 | 3 | 2 | 3 |
| sessions_duration_total_hour_dim4_grouped | 25 | 26 | 23 | 26 |
| sessions_new_installs_day_grouped | 2 | 3 | 2 | 3 |
| sessions_new_installs_hour_grouped | 15 | 16 | 14 | 17 |
| sessions_new_installs_day_dim4_grouped | 3 | 3 | 2 | 4 |
| sessions_new_installs_hour_dim4_grouped | 24 | 25 | 23 | 26 |
| events_count_day_ungrouped | 0 | 0 | 0 | 1 |
| events_count_hour_ungrouped | 2 | 2 | 2 | 2 |
| sessions_count_day_ungrouped | 0 | 0 | 0 | 0 |
| sessions_count_hour_ungrouped | 2 | 2 | 2 | 3 |
| sessions_duration_total_day_ungrouped | 0 | 0 | 0 | 0 |
| sessions_duration_total_hour_ungrouped | 2 | 3 | 2 | 3 |
| sessions_new_installs_day_ungrouped | 0 | 0 | 0 | 0 |
| sessions_active_installs_day_ungrouped | 33 | 34 | 32 | 36 |
| sessions_active_installs_day_dim4_grouped | 9 | 10 | 8 | 10 |
| sessions_active_installs_day_dim4_filtered | 4 | 5 | 4 | 5 |
| sessions_new_installs_hour_ungrouped | 2 | 2 | 2 | 3 |