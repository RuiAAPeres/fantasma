# Fantasma Derived Metrics SLO: reads-30d

- seed_ingest: 900000 events in 35143ms (25609.15 events/s)
- event_metrics_ready_ms: 62ms
- session_metrics_ready_ms: 14526ms
- derived_metrics_ready_ms: 14526ms
- Budget: PASS

| Query | p50 (ms) | p95 (ms) | min (ms) | max (ms) |
| --- | ---: | ---: | ---: | ---: |
| events_count_day_grouped | 4 | 6 | 3 | 6 |
| events_count_hour_grouped | 32 | 37 | 30 | 45 |
| sessions_count_day_grouped | 2 | 3 | 2 | 3 |
| sessions_count_hour_grouped | 14 | 16 | 13 | 19 |
| sessions_duration_total_day_grouped | 2 | 3 | 2 | 3 |
| sessions_duration_total_hour_grouped | 14 | 15 | 13 | 18 |
| sessions_new_installs_day_grouped | 2 | 4 | 2 | 5 |
| sessions_new_installs_hour_grouped | 14 | 15 | 13 | 17 |
| events_count_day_ungrouped | 0 | 1 | 0 | 1 |
| events_count_hour_ungrouped | 2 | 3 | 2 | 4 |
| sessions_count_day_ungrouped | 0 | 0 | 0 | 1 |
| sessions_count_hour_ungrouped | 2 | 3 | 2 | 3 |
| sessions_duration_total_day_ungrouped | 0 | 0 | 0 | 1 |
| sessions_duration_total_hour_ungrouped | 2 | 4 | 2 | 5 |
| sessions_new_installs_day_ungrouped | 0 | 0 | 0 | 0 |
| sessions_new_installs_hour_ungrouped | 2 | 3 | 2 | 3 |