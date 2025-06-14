# Performance Analysis

This project includes utilities for evaluating throughput and database efficiency.

## Benchmark Suite
- `benchmarks/benchmark_pipeline.py` runs a simple sequential benchmark of claim processing.
- `benchmarks/load_test.py` executes multiple concurrent workers to simulate load.

Run either script directly with Python to measure execution time.

## Resource Monitoring
`src/monitoring/resource_monitor.py` exposes helpers to track CPU and memory usage. Call
`start()` at application startup to begin collecting metrics which are available from the
`/metrics` endpoint.

## Query Analytics
`src/analysis/query_stats.py` summarizes average query latency using metrics gathered at
runtime. Execute it after processing to view the summary:

```bash
python -m src.analysis.query_stats
```

### Query Plans
`src/analysis/query_plan.py` provides helpers to inspect execution plans using
the database's `EXPLAIN` command. Call `explain()` with a `PostgresDatabase`
instance and use `has_seq_scan()` to detect sequential scans that may indicate
missing indexes.

### Automatic Index Recommendations
`src/analysis/index_recommender.py` analyzes recorded query times and plans to
suggest indexes for slow statements. Run the recommender after a workload to
print suggested `CREATE INDEX` commands.

### Connection Pool Multiplexing
Connection pool sizes can be tuned in `config.yaml`. Increasing the pool allows
more tasks to share connections for greater throughput.
`src/monitoring/pool_monitor.py` periodically records the number of connections
in use so you can visualize pool saturation over time.

## Performance Tuning
- **Batch Size**: The `processing.batch_size` setting controls how many claims are processed at once. Increase this value cautiously to avoid exhausting database connections.
- **Concurrency**: Run multiple worker processes to take advantage of multi-core servers. Monitor CPU usage to find the optimal number of workers.
- **Insert Workers**: `processing.insert_workers` controls how many database connections are used for bulk inserts.
- **Pool Sizes**: `postgresql.min_pool_size`, `postgresql.max_pool_size` and `sqlserver.pool_size` determine how many connections can be multiplexed.
- **Caching**: Enable Redis caching in `config.yaml` to reduce duplicate RVU lookups.
- **Database Indexes**: Ensure indexes exist on frequently queried columns such as `claims.claim_id` and `failed_claims.failed_at`.
- **Model Loading**: Place the ML model on local disk for faster startup and prediction latency.
