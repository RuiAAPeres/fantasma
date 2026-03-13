# Fantasma Benchmark: repair-path (heavy)

- seed_ingest: 3000 events in 99ms (30164.78 events/s)
- repair_ingest: 1200 events in 45ms (26631.97 events/s)
- seed_ready: 1684ms
- repair_ready: 1148ms

| Query | p50 (ms) | p95 (ms) | min (ms) | max (ms) |
| --- | ---: | ---: | ---: | ---: |
| events_day_dim2 | 1 | 2 | 1 | 2 |
| events_hour_dim2 | 4 | 6 | 3 | 6 |
| sessions_count_day | 0 | 0 | 0 | 0 |
| sessions_duration_total_day | 0 | 0 | 0 | 1 |