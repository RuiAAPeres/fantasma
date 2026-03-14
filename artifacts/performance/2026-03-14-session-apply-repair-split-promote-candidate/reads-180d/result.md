# Fantasma Derived Metrics SLO: reads-180d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 2

- seed_ingest: 5400000 events in 245157ms (22026.64 events/s)
- event_metrics_ready_ms: 730ms
- session_metrics_ready_ms: 45562ms
- derived_metrics_ready_ms: 45562ms
- Budget: PASS

| Query | p50 (ms) | p95 (ms) | min (ms) | max (ms) |
| --- | ---: | ---: | ---: | ---: |
| events_count_day_grouped | 16 | 17 | 15 | 19 |
| events_count_hour_grouped | 173 | 183 | 169 | 196 |
| events_count_day_dim4_grouped | 14 | 18 | 12 | 21 |
| events_count_hour_dim4_grouped | 142 | 149 | 138 | 155 |
| sessions_count_day_grouped | 7 | 8 | 6 | 9 |
| sessions_count_hour_grouped | 74 | 83 | 73 | 84 |
| sessions_duration_total_day_grouped | 7 | 7 | 6 | 7 |
| sessions_duration_total_hour_grouped | 78 | 90 | 73 | 112 |
| sessions_new_installs_day_grouped | 8 | 14 | 6 | 17 |
| sessions_new_installs_hour_grouped | 79 | 104 | 74 | 129 |
| events_count_day_ungrouped | 1 | 1 | 1 | 3 |
| events_count_hour_ungrouped | 23 | 32 | 13 | 97 |
| sessions_count_day_ungrouped | 3 | 6 | 1 | 9 |
| sessions_count_hour_ungrouped | 23 | 32 | 14 | 37 |
| sessions_duration_total_day_ungrouped | 3 | 4 | 1 | 5 |
| sessions_duration_total_hour_ungrouped | 21 | 28 | 13 | 31 |
| sessions_new_installs_day_ungrouped | 2 | 3 | 1 | 4 |
| sessions_new_installs_hour_ungrouped | 17 | 29 | 12 | 41 |