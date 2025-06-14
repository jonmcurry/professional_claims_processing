# Professional Claims Processing

## Overview
This project builds a high-performance claims processing system with an integrated machine learning (ML) model for filter prediction. It fetches claims from a PostgreSQL staging database, validates them, applies rules and ML-based filtering and then inserts the results into a SQL Server production database. Caching, asynchronous processing and connection pooling are used to maximize throughput.

### Performance Optimizations
- The pipeline processes streamed claims in parallel using worker tasks.
- Bulk inserts utilize multiple connections for higher throughput.
- SQL Server bulk inserts use table-valued parameters to minimize
  network overhead.
- PostgreSQL COPY is leveraged for high-volume reads.
- Query plans can be inspected via `src/analysis/query_plan.py` for tuning.
- Connection pools are pre-warmed and configured with larger defaults
  (PostgreSQL 20-100, SQL Server pool size 20) for improved multiplexing.
- Connection pool metrics are monitored by `pool_monitor` to spot saturation.
- Query timings are tracked with `query_tracker` for performance analysis.
- Slow queries can be fed to the `index_recommender` to generate index suggestions.
- Proper indexes and columnstore indexes are included in the
  provided schema scripts for optimized analytics queries.
- The `partition_historical_data` script partitions old claim records by year.

### Scaling Enhancements
- `ShardedDatabase` distributes writes across multiple PostgreSQL instances using a hash of the shard key.
- `ShardedClaimsProcessor` splits incoming claims by facility and runs each shard's pipeline in parallel for higher throughput.
- SQL Server queries now use a read-through cache to avoid repeated lookups.
- Materialized views defined in `sql/materialized_views.sql` accelerate analytics queries.
- A lightweight `ChangeDataCapture` poller streams row changes for real-time synchronization.

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for a high-level system diagram.

## Requirements
- Python 3.10 or newer
- Recommended: create a virtual environment

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
# The cryptography package is required for encryption support.
# It is included in the requirements file but must be installed for the
# application to start correctly.
pip install pre-commit
pre-commit install
```

Alternatively run `scripts/setup_dev.sh` to perform these steps automatically.

## Initial Database Setup
### PostgreSQL
1. Create a database named `staging_process`.
2. Apply the tables described in [docs/SCHEMA.md](docs/SCHEMA.md).
 3. Run the migrations with `alembic upgrade head` to apply updates.

### SQL Server
1. Create a database named `smart_pro_claims`.
2. Apply the tables described in [docs/SCHEMA.md](docs/SCHEMA.md).

Update `config.yaml` with your connection details.
If you have a read replica for PostgreSQL, set `replica_host` and `replica_port` in that file.

Set the `APP_ENV` environment variable to load `config.<environment>.yaml` or
`APP_CONFIG` to specify an explicit configuration file. When neither is set,
`config.yaml` is used.

The `cache` section can enable Redis for distributed RVU caching. Set `redis_url`
to your server and list common `warm_rvu_codes` to pre-populate the cache during
startup. Predictive warming uses recent access patterns to prefetch the next few
codes as defined by `predictive_ahead`. Cached RVU entries can be invalidated on
data updates and cache hit/miss ratios are exported via the metrics endpoint.
The `features` section allows toggling optional functionality like caching and model monitoring.

## ML Model
The processing pipeline loads a scikit-learn model using the path specified in the `model.path` setting of `config.yaml`. Train your own model or obtain the file from the maintainers and update the configuration with the correct location.
Model version and optional A/B test paths can also be configured. Basic model performance metrics are tracked using the in-memory metrics registry.

To generate a sample model for development, run:
```
python src/models/train_model.py
```
This creates the model file at the configured path.

## Usage
After installing dependencies and setting up the databases and model, run:

```bash
python -m src.processing.main
```

## Web UI
Run the FastAPI server to view failed claims and monitor real-time sync status:

```bash
uvicorn src.web.app:app --reload
```

The `/failed_claims` page lists recent failed claims and the `/status` endpoint
returns processing counts so you can display a real-time indicator in the UI.
Database and query metrics are exposed from the `/metrics` endpoint for
operational monitoring. Full API documentation is available at
`/docs` when the server is running. The raw OpenAPI specification can be
downloaded from `/openapi.json`.

## Tests
Tests use `pytest`.

```bash
pip install pytest
PYTHONPATH=. pytest
```

Add new tests under the `tests/` directory using files named `test_*.py`.

## Benchmarks
Simple performance benchmarks live in the `benchmarks/` directory.
Run the pipeline benchmark script with:

```bash
python benchmarks/benchmark_pipeline.py
```
Additional load testing utilities are available in `benchmarks/load_runner.py`.
See [docs/PERFORMANCE.md](docs/PERFORMANCE.md) for resource monitoring and query
analytics instructions.

## Contributing
Contributions are welcome! Please open an issue to discuss any changes. Ensure tests pass before submitting a pull request and follow PEP 8 style conventions.
Pre-commit hooks are configured to run linting tools automatically. After cloning the repository run `pre-commit install` (or use `scripts/setup_dev.sh`).

For detailed database schema information see [docs/SCHEMA.md](docs/SCHEMA.md).
Details on how old claims are archived can be found in
[docs/DATA_MANAGEMENT.md](docs/DATA_MANAGEMENT.md).

## Security and Compliance
This project includes basic security features to protect sensitive claim data.

- **PII/PHI Protection**: Failed claim records are stored with the raw data encrypted using
  a key from `config.yaml`.
- **Data Access Controls**: All web endpoints require an API key via the `X-API-Key` header.
- **Encryption**: The `encryption_key` from the security section of `config.yaml` is used to
  encrypt sensitive payloads before persisting them.
- **HIPAA Compliance**: See [docs/HIPAA_COMPLIANCE.md](docs/HIPAA_COMPLIANCE.md) for a checklist
  of recommended practices.
- **Automated Retention**: The helper script `src/maintenance/enforce_retention_policy.py`
  can be scheduled to enforce data retention policies.
- **Security Scanning**: Run `bandit -r src -ll` to perform static security analysis. See
  [docs/SECURITY_SCANNING.md](docs/SECURITY_SCANNING.md) for details.
- **RBAC**: Roles now include `auditor`, `user`, and `admin`. Auditor access is read-only.

## Error Handling
The system now features a global exception handler for the FastAPI application.
Errors are categorized and recorded in the `failure_category` field for easier
analysis. Failed operations are routed to a dead letter queue with automatic
reprocessing attempts. Failed claims enter a priority based retry queue where
automated repair is attempted before re‑insertion. Manual review workflows can
assign claims to users and record resolution actions. Resolution metrics are
tracked via the `/metrics` endpoint. Database inserts employ compensation
transactions to remove partially inserted records when an error occurs.

## Claim Processing Enhancements
The system now supports claim amendments using the `amend_claim` method and a
priority queue for high‑value claims. Batch progress information is exposed from
the `/batch_status` endpoint, and `process_partial_claim` allows partial claim
updates without reprocessing the full record.

## Analytics
Additional utilities are provided to understand failed claims and revenue impact.
- `src/analysis/revenue.py` calculates potential revenue loss from failures.
- `src/analysis/failure_patterns.py` aggregates and ranks failure reasons.
- `src/analysis/failure_predictor.py` offers a lightweight predictor for claim
  failure probability with a fallback when scikit-learn is unavailable.
- `src/analysis/trending.py` tracks moving averages and simple trends for any
  numeric metric.


## Operations
- See `migrations/README.md` for managing database migrations. Test migrations in a staging environment before production.
- Monitoring setup is documented in [docs/MONITORING.md](docs/MONITORING.md).
- Example alert rules are provided in [docs/ALERTING.md](docs/ALERTING.md).
- Backup and restore instructions live in [docs/BACKUP_RESTORE.md](docs/BACKUP_RESTORE.md) with a helper script under `src/maintenance/backup_restore.py`.
- Deployment steps are covered in [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md).
- Operational runbook tasks can be found in [docs/OPERATIONS_RUNBOOK.md](docs/OPERATIONS_RUNBOOK.md).
- Common troubleshooting tips live in [docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md).
- The project includes a `Dockerfile` and Kubernetes manifest under `k8s/` for containerized deployments.
- Continuous integration runs via GitHub Actions defined in `.github/workflows/ci.yml`.
