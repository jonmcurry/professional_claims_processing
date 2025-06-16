# Monitoring Setup

The application exposes metrics from the `/metrics` endpoint of the FastAPI server. Export these metrics to Prometheus and visualize them with Grafana.

## Key Metrics
- `postgres_query_ms`: PostgreSQL query latency
- `claims_processed`: number of successfully processed claims
- `claims_failed`: number of failed claims
- `postgres_pool_in_use`: connections used in the pool
- `postgres_pool_max`: max connections allowed
- `sqlserver_pool_size`: number of available SQL Server connections
- `logging_overhead_ms`: time spent writing a log entry
- `rvu_cache_hit_ratio`: proportion of RVU cache lookups served from cache
- `rvu_cache_miss_ratio`: proportion of lookups resulting in a database fetch
- `postgres_query_p99_ms`: 99th percentile PostgreSQL query latency
- `sqlserver_query_p99_ms`: 99th percentile SQL Server query latency
- `errors_validation`: count of validation errors
- `errors_database`: count of database errors
- `cpu_usage_avg`: moving average CPU utilization
- `memory_usage_avg`: moving average memory usage
- `batch_processing_rate_per_sec`: processed claims per second for last batch
- `dynamic_batch_size`: batch size chosen based on system load
- `cpu_capacity_forecast`: predicted CPU utilization for the next interval
- `memory_capacity_forecast`: predicted memory usage for the next interval
- `throughput_forecast`: predicted claims throughput

Create dashboards to track CPU, memory, error rates, request latency, and log metrics. Logs are forwarded to Logstash for ingestion into Elasticsearch and Kibana.

Query execution times are also tracked per statement type. Use `src/analysis/query_tracker.py` to view slow queries and feed them into the index recommender for optimization guidance.
