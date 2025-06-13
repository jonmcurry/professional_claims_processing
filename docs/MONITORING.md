# Monitoring Setup

The application exposes metrics from the `/metrics` endpoint of the FastAPI server. Export these metrics to Prometheus and visualize them with Grafana.

## Key Metrics
- `postgres_query_ms`: PostgreSQL query latency
- `claims_processed`: number of successfully processed claims
- `claims_failed`: number of failed claims
- `postgres_pool_in_use`: connections used in the pool
- `logging_overhead_ms`: time spent writing a log entry

Create dashboards to track CPU, memory, error rates, request latency, and log metrics. Logs are forwarded to Logstash for ingestion into Elasticsearch and Kibana.
