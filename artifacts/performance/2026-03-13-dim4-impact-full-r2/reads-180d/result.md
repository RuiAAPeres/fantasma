# Fantasma Derived Metrics SLO: reads-180d

- seed_ingest: 5400000 events in 175775ms (30721.07 events/s)
- event_metrics_ready_ms: 635ms
- session_metrics_ready_ms: 169084ms
- derived_metrics_ready_ms: 169084ms
- Budget: PASS

| Query | p50 (ms) | p95 (ms) | min (ms) | max (ms) |
| --- | ---: | ---: | ---: | ---: |
| events_count_day_grouped | 20 | 20 | 19 | 22 |
| events_count_hour_grouped | 195 | 199 | 193 | 272 |
| events_count_day_dim4_grouped | 12 | 13 | 12 | 14 |
| events_count_hour_dim4_grouped | 142 | 145 | 139 | 149 |
| sessions_count_day_grouped | 7 | 8 | 6 | 8 |
| sessions_count_hour_grouped | 75 | 77 | 73 | 78 |
| sessions_duration_total_day_grouped | 7 | 8 | 6 | 8 |
| sessions_duration_total_hour_grouped | 75 | 77 | 73 | 80 |
| sessions_new_installs_day_grouped | 7 | 8 | 6 | 8 |
| sessions_new_installs_hour_grouped | 75 | 77 | 73 | 79 |
| events_count_day_ungrouped | 1 | 1 | 1 | 1 |
| events_count_hour_ungrouped | 11 | 11 | 10 | 12 |
| sessions_count_day_ungrouped | 1 | 1 | 1 | 1 |
| sessions_count_hour_ungrouped | 11 | 12 | 11 | 13 |
| sessions_duration_total_day_ungrouped | 1 | 1 | 1 | 1 |
| sessions_duration_total_hour_ungrouped | 11 | 12 | 11 | 12 |
| sessions_new_installs_day_ungrouped | 1 | 1 | 1 | 1 |
| sessions_new_installs_hour_ungrouped | 11 | 12 | 10 | 13 |