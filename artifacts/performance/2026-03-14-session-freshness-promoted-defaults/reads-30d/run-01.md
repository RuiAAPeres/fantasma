# Fantasma Derived Metrics SLO: reads-30d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 4

- seed_ingest: 900000 events in 38732ms (23236.33 events/s)
- event_metrics_ready_ms: 177ms
- session_metrics_ready_ms: 2621ms
- derived_metrics_ready_ms: 2621ms
- Budget: PASS

| Query | p50 (ms) | p95 (ms) | min (ms) | max (ms) |
| --- | ---: | ---: | ---: | ---: |
| events_count_day_grouped | 8 | 10 | 6 | 12 |
| events_count_hour_grouped | 30 | 32 | 29 | 35 |
| events_count_day_dim4_grouped | 15 | 17 | 9 | 23 |
| events_count_hour_dim4_grouped | 37 | 39 | 34 | 44 |
| sessions_count_day_grouped | 3 | 6 | 2 | 6 |
| sessions_count_hour_grouped | 16 | 18 | 15 | 20 |
| sessions_duration_total_day_grouped | 5 | 7 | 4 | 8 |
| sessions_duration_total_hour_grouped | 16 | 17 | 15 | 21 |
| sessions_new_installs_day_grouped | 4 | 7 | 2 | 17 |
| sessions_new_installs_hour_grouped | 17 | 23 | 15 | 27 |
| events_count_day_ungrouped | 1 | 1 | 0 | 1 |
| events_count_hour_ungrouped | 5 | 6 | 4 | 7 |
| sessions_count_day_ungrouped | 1 | 2 | 0 | 5 |
| sessions_count_hour_ungrouped | 7 | 9 | 4 | 12 |
| sessions_duration_total_day_ungrouped | 1 | 2 | 1 | 3 |
| sessions_duration_total_hour_ungrouped | 6 | 8 | 4 | 9 |
| sessions_new_installs_day_ungrouped | 1 | 2 | 1 | 3 |
| sessions_new_installs_hour_ungrouped | 6 | 8 | 4 | 10 |