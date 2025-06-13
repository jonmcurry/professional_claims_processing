# System Architecture

The claims processing pipeline consists of distinct components interacting through databases and a messaging layer.

```mermaid
flowchart TD
    A[PostgreSQL Staging DB] -->|fetch claims| B[Processing Worker]
    B --> C[Rules Engine]
    B --> D[ML Model]
    C --> B
    D --> B
    B -->|valid claims| E[SQL Server Production DB]
    B -->|failed claims| F[Dead Letter Queue]
    F -->|reprocess| B
```

The FastAPI application exposes operational endpoints for monitoring and reviewing failed claims. Caching and connection pools improve throughput under load.
