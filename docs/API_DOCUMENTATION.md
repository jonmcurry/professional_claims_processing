# API Documentation

The FastAPI application automatically generates an OpenAPI specification and interactive Swagger UI.

- **Swagger UI**: visit `http://<host>:8000/docs` while the service is running to explore the endpoints and schemas.
- **OpenAPI JSON**: the raw specification is available at `http://<host>:8000/openapi.json`.

The main API endpoints include:
- `/failed_claims` – view recently failed claims.
- `/status` – processing statistics for the current run.
- `/batch_status` – details about the active batch.
- `/health` and `/readiness` – service health checks.
- `/compliance/dashboard` – summary of audit logs and retention status.

Authentication requires an API key in the `X-API-Key` header. Example request using `curl`:
```bash
curl -H "X-API-Key: <key>" http://<host>:8000/status
```

### Compliance Dashboard Screenshot

![Compliance Dashboard](images/compliance_dashboard.png)