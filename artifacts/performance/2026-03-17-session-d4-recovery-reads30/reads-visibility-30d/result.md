# Fantasma Derived Metrics SLO: reads-visibility-30d

- Run config:
  - mode: iterative
  - publication_repetitions: 1
  - worker_session_batch_size: 1000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2
  - worker_idle_poll_interval_ms: 250

- seed_ingest: 900000 events in 33035ms (27243.01 events/s)
- event_metrics_ready_ms: 234ms
- session_metrics_ready_ms: 178797ms
- derived_metrics_ready_ms: 178797ms
- Budget: PASS

| Query | p50 (ms) | p95 (ms) | min (ms) | max (ms) |
| --- | ---: | ---: | ---: | ---: |
| events_count_day_grouped | 4 | 5 | 3 | 5 |
| events_count_hour_grouped | 29 | 30 | 28 | 30 |
| events_count_day_dim4_grouped | 3 | 4 | 2 | 4 |
| events_count_hour_dim4_grouped | 25 | 26 | 24 | 27 |
| sessions_count_day_grouped | 2 | 3 | 2 | 3 |
| sessions_count_hour_grouped | 15 | 16 | 14 | 18 |
| sessions_count_day_dim4_grouped | 3 | 4 | 2 | 4 |
| sessions_count_hour_dim4_grouped | 24 | 25 | 24 | 26 |
| sessions_duration_total_day_grouped | 2 | 3 | 2 | 4 |
| sessions_duration_total_hour_grouped | 15 | 16 | 14 | 16 |
| sessions_duration_total_day_dim4_grouped | 3 | 4 | 2 | 4 |
| sessions_duration_total_hour_dim4_grouped | 24 | 25 | 23 | 25 |
| sessions_new_installs_day_grouped | 2 | 3 | 2 | 3 |
| sessions_new_installs_hour_grouped | 15 | 15 | 14 | 17 |
| sessions_new_installs_day_dim4_grouped | 3 | 4 | 2 | 5 |
| sessions_new_installs_hour_dim4_grouped | 24 | 25 | 24 | 26 |
| events_count_day_ungrouped | 0 | 0 | 0 | 0 |
| events_count_hour_ungrouped | 2 | 2 | 2 | 2 |
| sessions_count_day_ungrouped | 0 | 0 | 0 | 0 |
| sessions_count_hour_ungrouped | 2 | 2 | 2 | 2 |
| sessions_duration_total_day_ungrouped | 0 | 0 | 0 | 0 |
| sessions_duration_total_hour_ungrouped | 2 | 2 | 2 | 2 |
| sessions_new_installs_day_ungrouped | 0 | 0 | 0 | 0 |
| sessions_active_installs_day_ungrouped | 4 | 4 | 3 | 5 |
| sessions_active_installs_day_dim4_grouped | 6 | 6 | 5 | 9 |
| sessions_active_installs_day_dim4_filtered | 3 | 3 | 3 | 4 |
| sessions_new_installs_hour_ungrouped | 2 | 3 | 2 | 3 |