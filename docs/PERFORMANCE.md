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

## Performance Tuning
- **Batch Size**: The `processing.batch_size` setting controls how many claims are processed at once. Increase this value cautiously to avoid exhausting database connections.
- **Concurrency**: Run multiple worker processes to take advantage of multi-core servers. Monitor CPU usage to find the optimal number of workers.
- **Caching**: Enable Redis caching in `config.yaml` to reduce duplicate RVU lookups.
- **Database Indexes**: Ensure indexes exist on frequently queried columns such as `claims.claim_id` and `failed_claims.failed_at`.
- **Model Loading**: Place the ML model on local disk for faster startup and prediction latency.
