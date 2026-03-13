# Fantasma Derived Metrics SLO: reads-30d

- seed_ingest: 900000 events in 33322ms (27008.90 events/s)
- event_metrics_ready_ms: 58ms
- session_metrics_ready_ms: 10613ms
- derived_metrics_ready_ms: 10613ms
- Budget: PASS

| Query | p50 (ms) | p95 (ms) | min (ms) | max (ms) |
| --- | ---: | ---: | ---: | ---: |
| events_count_day_grouped | 5 | 6 | 3 | 6 |
| events_count_hour_grouped | 31 | 33 | 30 | 37 |
| sessions_count_day_grouped | 2 | 2 | 2 | 3 |
| sessions_count_hour_grouped | 14 | 14 | 13 | 19 |
| sessions_duration_total_day_grouped | 2 | 3 | 2 | 3 |
| sessions_duration_total_hour_grouped | 14 | 15 | 13 | 15 |
| sessions_new_installs_day_grouped | 2 | 4 | 2 | 5 |
| sessions_new_installs_hour_grouped | 14 | 14 | 13 | 17 |
| events_count_day_ungrouped | 0 | 1 | 0 | 1 |
| events_count_hour_ungrouped | 2 | 4 | 2 | 4 |
| sessions_count_day_ungrouped | 1 | 1 | 0 | 1 |
| sessions_count_hour_ungrouped | 3 | 4 | 2 | 5 |
| sessions_duration_total_day_ungrouped | 0 | 0 | 0 | 0 |
| sessions_duration_total_hour_ungrouped | 3 | 4 | 2 | 5 |
| sessions_new_installs_day_ungrouped | 1 | 1 | 0 | 1 |
| sessions_new_installs_hour_ungrouped | 2 | 5 | 2 | 6 |