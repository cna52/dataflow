# Local pipeline benchmarks

Run the profiles with:

```bash
docker compose run --rm --entrypoint dataflow airflow-init benchmark-pipeline --sizes 10000 100000 500000
```

Each size uses a fresh local PostgreSQL database and a separate data directory. The command seeds synthetic data, runs a full pipeline, applies the deterministic update scenario, runs incrementally, then runs a full rebuild. It checks that incremental and rebuild order counts and revenue match. Each pipeline includes ingestion, raw validation, Spark processing, DuckDB loading, dbt models and tests, and publication. The benchmark removes its databases and datasets after recording results in the ignored `benchmark_results/` directory.

The figures below are from one run on a 16 GB Apple Silicon laptop with Docker limited to about 8 GB. Durations are pipeline wall times, excluding source generation. Memory is sampled from the benchmark container's cgroup and includes Python and the Spark JVM. DuckDB latency is one `sum(revenue_cents), count(*)` query; it is not a repeated-query distribution.

| Seed orders | Full | Incremental update | Full rebuild after update | Joined rows rewritten by update | dbt tests |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 10,000 | 34.1 s | 32.1 s | 33.1 s | 7,480 / 25,211 | 28 / 28 |
| 100,000 | 51.9 s | 37.5 s | 60.0 s | 79,925 / 250,442 | 28 / 28 |
| 500,000 | 106.3 s | 44.2 s | 128.2 s | 390,891 / 1,249,899 | 28 / 28 |

| Seed orders | Peak container memory: full / update / rebuild | DuckDB query: full / update / rebuild |
| ---: | ---: | ---: |
| 10,000 | 1.84 / 1.80 / 2.27 GiB | 3.7 / 6.9 / 4.5 ms |
| 100,000 | 2.27 / 2.17 / 3.08 GiB | 23.3 / 22.2 / 25.2 ms |
| 500,000 | 4.47 / 4.07 / 5.01 GiB | 29.2 / 36.2 / 33.6 ms |

The update scenario changes up to 100 orders and creates one new order, event, and product category version. Spark compares the effective category at each order's purchase time and skips historical orders whose category is unchanged. Even with bucketed snapshots, a change spread across many order IDs touches many buckets. The update rewrote about 30–32% of joined rows across these sizes. Spark startup and scans dominate at 10,000 orders, while the avoided work produces a clearer time difference at 100,000 and 500,000 orders. These results show local behavior for this data distribution; they do not establish distributed scalability or claim that Spark is faster than DuckDB on one machine.

For failure recovery, `dataflow verify-recovery` used a separate 100-order dataset. An invalid event blocked publication; a corrected event cleared quarantine; and rerunning the repaired batch kept totals unchanged. Invalid pricing then stopped Spark before it could write a processed snapshot. After the pricing file was restored, the next run replayed the failed raw batch and applied its historical cancellation, moving daily revenue by 45,144 cents. A backfill for that date reconciled to the same daily revenue.
