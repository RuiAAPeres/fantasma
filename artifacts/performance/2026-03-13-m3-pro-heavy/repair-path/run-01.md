# Fantasma Benchmark: repair-path (heavy)

- seed_ingest: 3000 events in 111ms (26932.84 events/s)
- repair_ingest: 1200 events in 44ms (27105.04 events/s)
- seed_ready: 1690ms
- repair_ready: 1145ms

| Query | p50 (ms) | p95 (ms) | min (ms) | max (ms) |
| --- | ---: | ---: | ---: | ---: |
| events_day_dim2 | 1 | 2 | 1 | 2 |
| events_hour_dim2 | 4 | 5 | 3 | 7 |
| sessions_count_day | 0 | 0 | 0 | 0 |
| sessions_duration_total_day | 0 | 0 | 0 | 1 |