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
