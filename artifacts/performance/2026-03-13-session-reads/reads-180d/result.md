# Fantasma Derived Metrics SLO: reads-180d

- seed_ingest: 5400000 events in 205286ms (26304.72 events/s)
- event_metrics_ready_ms: 272ms
- session_metrics_ready_ms: 64953ms
- derived_metrics_ready_ms: 64953ms
- Budget: PASS

| Query | p50 (ms) | p95 (ms) | min (ms) | max (ms) |
| --- | ---: | ---: | ---: | ---: |
| events_count_day_grouped | 18 | 19 | 17 | 23 |
| events_count_hour_grouped | 189 | 191 | 184 | 194 |
| sessions_count_day_grouped | 7 | 7 | 6 | 8 |
| sessions_count_hour_grouped | 73 | 75 | 71 | 82 |
| sessions_duration_total_day_grouped | 7 | 8 | 6 | 9 |
| sessions_duration_total_hour_grouped | 74 | 76 | 71 | 78 |
| sessions_new_installs_day_grouped | 6 | 7 | 6 | 7 |
| sessions_new_installs_hour_grouped | 73 | 75 | 71 | 77 |
| events_count_day_ungrouped | 1 | 1 | 1 | 1 |
| events_count_hour_ungrouped | 11 | 11 | 10 | 11 |
| sessions_count_day_ungrouped | 1 | 1 | 1 | 1 |
| sessions_count_hour_ungrouped | 11 | 11 | 10 | 12 |
| sessions_duration_total_day_ungrouped | 1 | 1 | 1 | 1 |
| sessions_duration_total_hour_ungrouped | 11 | 11 | 10 | 11 |
| sessions_new_installs_day_ungrouped | 1 | 1 | 1 | 1 |
| sessions_new_installs_hour_ungrouped | 10 | 11 | 10 | 15 |