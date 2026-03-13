# Fantasma Derived Metrics SLO: reads-90d

- seed_ingest: 2700000 events in 111410ms (24234.68 events/s)
- event_metrics_ready_ms: 432ms
- session_metrics_ready_ms: 81960ms
- derived_metrics_ready_ms: 81960ms
- Budget: PASS

| Query | p50 (ms) | p95 (ms) | min (ms) | max (ms) |
| --- | ---: | ---: | ---: | ---: |
| events_count_day_grouped | 10 | 11 | 9 | 11 |
| events_count_hour_grouped | 97 | 98 | 96 | 100 |
| events_count_day_dim4_grouped | 6 | 7 | 6 | 7 |
| events_count_hour_dim4_grouped | 73 | 75 | 71 | 79 |
| sessions_count_day_grouped | 4 | 5 | 3 | 5 |
| sessions_count_hour_grouped | 38 | 39 | 37 | 40 |
| sessions_duration_total_day_grouped | 4 | 5 | 3 | 5 |
| sessions_duration_total_hour_grouped | 38 | 40 | 37 | 47 |
| sessions_new_installs_day_grouped | 4 | 5 | 3 | 5 |
| sessions_new_installs_hour_grouped | 38 | 40 | 37 | 44 |
| events_count_day_ungrouped | 1 | 1 | 0 | 1 |
| events_count_hour_ungrouped | 6 | 6 | 5 | 7 |
| sessions_count_day_ungrouped | 1 | 1 | 0 | 1 |
| sessions_count_hour_ungrouped | 6 | 6 | 6 | 7 |
| sessions_duration_total_day_ungrouped | 1 | 1 | 0 | 1 |
| sessions_duration_total_hour_ungrouped | 6 | 6 | 6 | 7 |
| sessions_new_installs_day_ungrouped | 0 | 1 | 0 | 1 |
| sessions_new_installs_hour_ungrouped | 6 | 6 | 5 | 6 |