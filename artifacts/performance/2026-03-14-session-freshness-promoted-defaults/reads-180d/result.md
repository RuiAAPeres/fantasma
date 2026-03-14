# Fantasma Derived Metrics SLO: reads-180d

- Run config:
  - repetitions_30d: 3
  - worker_session_batch_size: 5000
  - worker_event_batch_size: 5000
  - worker_session_incremental_concurrency: 8
  - worker_session_repair_concurrency: 4

- seed_ingest: 5400000 events in 268665ms (20099.35 events/s)
- event_metrics_ready_ms: 586ms
- session_metrics_ready_ms: 678ms
- derived_metrics_ready_ms: 678ms
- Budget: PASS

| Query | p50 (ms) | p95 (ms) | min (ms) | max (ms) |
| --- | ---: | ---: | ---: | ---: |
| events_count_day_grouped | 18 | 20 | 17 | 21 |
| events_count_hour_grouped | 182 | 186 | 177 | 194 |
| events_count_day_dim4_grouped | 14 | 15 | 13 | 16 |
| events_count_hour_dim4_grouped | 148 | 152 | 142 | 155 |
| sessions_count_day_grouped | 9 | 10 | 7 | 10 |
| sessions_count_hour_grouped | 80 | 81 | 77 | 82 |
| sessions_duration_total_day_grouped | 10 | 10 | 7 | 11 |
| sessions_duration_total_hour_grouped | 81 | 82 | 76 | 83 |
| sessions_new_installs_day_grouped | 10 | 10 | 7 | 10 |
| sessions_new_installs_hour_grouped | 81 | 94 | 77 | 103 |
| events_count_day_ungrouped | 1 | 1 | 1 | 2 |
| events_count_hour_ungrouped | 12 | 14 | 11 | 15 |
| sessions_count_day_ungrouped | 1 | 1 | 1 | 2 |
| sessions_count_hour_ungrouped | 12 | 14 | 11 | 18 |
| sessions_duration_total_day_ungrouped | 1 | 2 | 1 | 3 |
| sessions_duration_total_hour_ungrouped | 12 | 15 | 11 | 17 |
| sessions_new_installs_day_ungrouped | 1 | 2 | 1 | 3 |
| sessions_new_installs_hour_ungrouped | 12 | 14 | 11 | 16 |