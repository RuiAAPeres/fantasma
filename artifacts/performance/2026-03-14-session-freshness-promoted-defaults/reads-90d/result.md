# Fantasma Derived Metrics SLO: reads-90d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 4

- seed_ingest: 2700000 events in 91004ms (29668.91 events/s)
- event_metrics_ready_ms: 369ms
- session_metrics_ready_ms: 28253ms
- derived_metrics_ready_ms: 28253ms
- Budget: PASS

| Query | p50 (ms) | p95 (ms) | min (ms) | max (ms) |
| --- | ---: | ---: | ---: | ---: |
| events_count_day_grouped | 9 | 12 | 8 | 14 |
| events_count_hour_grouped | 92 | 102 | 85 | 108 |
| events_count_day_dim4_grouped | 7 | 10 | 6 | 12 |
| events_count_hour_dim4_grouped | 75 | 82 | 71 | 87 |
| sessions_count_day_grouped | 5 | 6 | 4 | 11 |
| sessions_count_hour_grouped | 43 | 49 | 38 | 54 |
| sessions_duration_total_day_grouped | 4 | 6 | 4 | 8 |
| sessions_duration_total_hour_grouped | 43 | 50 | 38 | 55 |
| sessions_new_installs_day_grouped | 5 | 6 | 4 | 8 |
| sessions_new_installs_hour_grouped | 41 | 49 | 36 | 54 |
| events_count_day_ungrouped | 0 | 1 | 0 | 1 |
| events_count_hour_ungrouped | 6 | 7 | 5 | 9 |
| sessions_count_day_ungrouped | 1 | 1 | 0 | 1 |
| sessions_count_hour_ungrouped | 6 | 7 | 5 | 9 |
| sessions_duration_total_day_ungrouped | 0 | 1 | 0 | 2 |
| sessions_duration_total_hour_ungrouped | 6 | 8 | 5 | 10 |
| sessions_new_installs_day_ungrouped | 0 | 1 | 0 | 1 |
| sessions_new_installs_hour_ungrouped | 6 | 7 | 5 | 9 |