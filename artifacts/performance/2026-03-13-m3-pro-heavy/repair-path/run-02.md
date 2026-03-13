# Fantasma Benchmark: repair-path (heavy)

- seed_ingest: 3000 events in 105ms (28516.09 events/s)
- repair_ingest: 1200 events in 44ms (26810.97 events/s)
- seed_ready: 1821ms
- repair_ready: 1158ms

| Query | p50 (ms) | p95 (ms) | min (ms) | max (ms) |
| --- | ---: | ---: | ---: | ---: |
| events_day_dim2 | 1 | 2 | 1 | 2 |
| events_hour_dim2 | 4 | 5 | 4 | 6 |
| sessions_count_day | 0 | 1 | 0 | 1 |
| sessions_duration_total_day | 0 | 0 | 0 | 0 |