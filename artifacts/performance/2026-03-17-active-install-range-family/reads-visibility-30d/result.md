# Fantasma Derived Metrics SLO: reads-visibility-30d

- Run config:
  - mode: iterative
  - publication_repetitions: 1
  - worker_session_batch_size: 1000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2
  - worker_idle_poll_interval_ms: 250

- seed_ingest: 900000 events in 38739ms (23231.91 events/s)
- event_metrics_ready_ms: 125ms
- session_metrics_ready_ms: 60624ms
- derived_metrics_ready_ms: 60624ms
- Budget: PASS

| Query | p50 (ms) | p95 (ms) | min (ms) | max (ms) |
| --- | ---: | ---: | ---: | ---: |
| events_count_day_grouped | 3 | 3 | 2 | 3 |
| events_count_hour_grouped | 22 | 22 | 21 | 24 |
| events_count_day_dim2_grouped | 1 | 1 | 1 | 1 |
| events_count_hour_dim2_grouped | 7 | 7 | 6 | 8 |
| sessions_count_day_grouped | 2 | 3 | 2 | 3 |
| sessions_count_hour_grouped | 15 | 16 | 14 | 17 |
| sessions_count_day_dim2_grouped | 1 | 2 | 1 | 2 |
| sessions_count_hour_dim2_grouped | 8 | 8 | 7 | 9 |
| sessions_duration_total_day_grouped | 2 | 3 | 2 | 3 |
| sessions_duration_total_hour_grouped | 16 | 16 | 14 | 17 |
| sessions_duration_total_day_dim2_grouped | 1 | 1 | 1 | 2 |
| sessions_duration_total_hour_dim2_grouped | 8 | 8 | 7 | 9 |
| sessions_new_installs_day_grouped | 2 | 3 | 2 | 3 |
| sessions_new_installs_hour_grouped | 16 | 17 | 14 | 18 |
| sessions_new_installs_day_dim2_grouped | 1 | 2 | 1 | 2 |
| sessions_new_installs_hour_dim2_grouped | 8 | 8 | 7 | 9 |
| events_count_day_ungrouped | 0 | 0 | 0 | 0 |
| events_count_hour_ungrouped | 2 | 2 | 2 | 2 |
| sessions_count_day_ungrouped | 0 | 0 | 0 | 0 |
| sessions_count_hour_ungrouped | 2 | 2 | 2 | 3 |
| sessions_duration_total_day_ungrouped | 0 | 0 | 0 | 0 |
| sessions_duration_total_hour_ungrouped | 2 | 2 | 2 | 3 |
| sessions_new_installs_day_ungrouped | 0 | 0 | 0 | 1 |
| sessions_active_installs_day_ungrouped | 3 | 3 | 2 | 3 |
| sessions_active_installs_day_dim2_grouped | 3 | 3 | 2 | 4 |
| sessions_active_installs_day_dim2_filtered | 4 | 4 | 3 | 5 |
| sessions_active_installs_week_ungrouped | 1 | 1 | 0 | 1 |
| sessions_active_installs_week_dim2_grouped | 1 | 1 | 1 | 1 |
| sessions_active_installs_week_dim2_filtered | 1 | 1 | 1 | 2 |
| sessions_active_installs_month_ungrouped | 1 | 1 | 0 | 1 |
| sessions_active_installs_month_dim2_grouped | 1 | 1 | 0 | 1 |
| sessions_active_installs_month_dim2_filtered | 0 | 1 | 0 | 1 |
| sessions_active_installs_year_ungrouped | 1 | 1 | 0 | 1 |
| sessions_active_installs_year_dim2_grouped | 1 | 1 | 0 | 1 |
| sessions_active_installs_year_dim2_filtered | 0 | 0 | 0 | 1 |
| sessions_new_installs_hour_ungrouped | 2 | 2 | 2 | 2 |