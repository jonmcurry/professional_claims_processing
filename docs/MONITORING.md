# Monitoring Setup

The application exposes metrics from the `/metrics` endpoint of the FastAPI server. Export these metrics to Prometheus and visualize them with Grafana.

## Key Metrics
- `postgres_query_ms`: PostgreSQL query latency
- `claims_processed`: number of successfully processed claims
- `claims_failed`: number of failed claims
- `postgres_pool_in_use`: connections used in the pool

Create dashboards to track CPU, memory, error rates, and request latency. Review dashboards regularly to ensure they capture the desired insights.
