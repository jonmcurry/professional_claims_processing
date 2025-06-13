# Professional Claims Processing

## Overview
This project builds a high-performance claims processing system with an integrated machine learning (ML) model for filter prediction. It fetches claims from a PostgreSQL staging database, validates them, applies rules and ML-based filtering and then inserts the results into a SQL Server production database. Caching, asynchronous processing and connection pooling are used to maximize throughput.

## Requirements
- Python 3.10 or newer
- Recommended: create a virtual environment

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

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
startup.

## ML Model
The processing pipeline loads a scikit-learn model using the path specified in the `model.path` setting of `config.yaml`. Train your own model or obtain the file from the maintainers and update the configuration with the correct location.

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
operational monitoring.

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

## Contributing
Contributions are welcome! Please open an issue to discuss any changes. Ensure tests pass before submitting a pull request and follow PEP 8 style conventions.

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

## Error Handling
The system now features a global exception handler for the FastAPI application.
Errors are categorized and recorded in the `failure_category` field for easier
analysis. Failed operations are routed to a dead letter queue with automatic
reprocessing attempts. Database inserts employ compensation transactions to
remove partially inserted records when an error occurs.

## Claim Processing Enhancements
The system now supports claim amendments using the `amend_claim` method and a
priority queue for high‑value claims. Batch progress information is exposed from
the `/batch_status` endpoint, and `process_partial_claim` allows partial claim
updates without reprocessing the full record.

