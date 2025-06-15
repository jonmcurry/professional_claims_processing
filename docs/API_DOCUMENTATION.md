# API Documentation

The FastAPI service exposes a REST API secured with API key authentication.
Interactive documentation is available when the service is running.

- **Swagger UI**: `http://<host>:8000/docs`
- **OpenAPI JSON**: `http://<host>:8000/openapi.json`

## Endpoints

| Method | Path | Description |
| ------ | ---- | ----------- |
| `GET` | `/failed_claims` | HTML view of the most recent claim failures |
| `GET` | `/api/failed_claims` | JSON list of failed claims |
| `GET` | `/status` | Processing statistics for the current run |
| `GET` | `/batch_status` | Detailed batch information |
| `GET` | `/health` | Health status of downstream services |
| `GET` | `/readiness` | Readiness probe for Kubernetes |
| `GET` | `/liveness` | Simple liveness probe |
| `GET` | `/metrics` | Prometheus formatted metrics |
| `GET` | `/profiling/start` | Begin CPU profiling |
| `GET` | `/profiling/stop` | Stop profiling and return stats |
| `GET` | `/compliance/dashboard` | Compliance metrics including audit counts, archive status, failure patterns, processing trends, and revenue impact |

### Example Request

```bash
curl -H "X-API-Key: <key>" http://<host>:8000/status
```

Responses are JSON encoded except for the `/failed_claims` HTML page.

