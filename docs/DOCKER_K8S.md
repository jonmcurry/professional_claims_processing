# Container and Kubernetes Deployment

This guide explains how to build a Docker image and deploy the service to Kubernetes.

## Build Docker Image
```bash
docker build -t claims-processing:latest .
```

## Run Locally
```bash
docker run -p 8000:8000 claims-processing:latest
```

## Kubernetes
Apply the deployment manifest in `k8s/deployment.yaml`:
```bash
kubectl apply -f k8s/deployment.yaml
```
This creates a Deployment and Service exposing the API on port 80.
