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

### SQL Server
1. Create a database named `smart_pro_claims`.
2. Apply the tables described in [docs/SCHEMA.md](docs/SCHEMA.md).

Update `config.yaml` with your connection details.

The `cache` section can enable Redis for distributed RVU caching. Set `redis_url`
to your server and list common `warm_rvu_codes` to pre-populate the cache during
startup.

## ML Model
The processing pipeline loads a scikit-learn model from `model.joblib` located in the project root. Train your own model or obtain the file from the maintainers and place it in this location before running the pipeline.

To generate a sample model for development, run:
```
python src/models/train_model.py
```
This creates `model.joblib` in the project root.

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

## Tests
Tests use `pytest`.

```bash
pip install pytest
pytest
```

Add new tests under the `tests/` directory using files named `test_*.py`.

## Contributing
Contributions are welcome! Please open an issue to discuss any changes. Ensure tests pass before submitting a pull request and follow PEP 8 style conventions.

For detailed database schema information see [docs/SCHEMA.md](docs/SCHEMA.md).

## Security and Compliance
This project includes basic security features to protect sensitive claim data.

- **PII/PHI Protection**: Failed claim records are stored with the raw data encrypted using
  a key from `config.yaml`.
- **Data Access Controls**: All web endpoints require an API key via the `X-API-Key` header.
- **Encryption**: The `encryption_key` from the security section of `config.yaml` is used to
  encrypt sensitive payloads before persisting them.

