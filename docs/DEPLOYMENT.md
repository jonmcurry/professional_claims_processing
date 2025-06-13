# Deployment Guide

This document outlines the basic steps to deploy the claims processing service in a production environment.

## Prerequisites
- Python 3.10+
- PostgreSQL and SQL Server instances
- A Redis server if distributed caching is enabled
- Access to the trained ML model file specified in `config.yaml`

## Steps
1. **Clone the repository** and install dependencies:
   ```bash
   git clone <repo-url>
   cd professional_claims_processing
   python -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```
2. **Configure the application** by editing `config.yaml` or providing an environment specific file via the `APP_ENV` or `APP_CONFIG` variables.
3. **Apply database migrations**:
   ```bash
   alembic upgrade head
   ```
4. **Start the processing service**:
   ```bash
   python -m src.processing.main
   ```
5. **Launch the FastAPI web UI** for monitoring and troubleshooting:
   ```bash
   uvicorn src.web.app:app --host 0.0.0.0 --port 8000
   ```

For containerized deployments you can base your Docker image on the official Python image and copy the application code into it. Make sure the working directory contains the model file and configuration prior to starting the service.

## Docker and Kubernetes
A reference `Dockerfile` is provided at the project root. Build the image with:
```bash
docker build -t claims-processing:latest .
```
Run it locally using:
```bash
docker run -p 8000:8000 claims-processing:latest
```
For Kubernetes deployments see `docs/DOCKER_K8S.md` and apply the manifest in `k8s/deployment.yaml`.
