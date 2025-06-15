# Troubleshooting Guide

This guide lists common operational issues and their resolutions.

## Database Connection Failures
- Verify network connectivity to PostgreSQL and SQL Server.
- Check credentials in `config.yaml` and environment variables.
- Review database logs for authentication errors.

## High Claim Failure Rate
- Inspect recent failed claims via `/failed_claims`.
- Validate that business rules and ML model versions are correct.
- Examine application logs for stack traces.

## Performance Degradation
- Monitor CPU and memory usage from the `/metrics` endpoint.
- Ensure the batch size and concurrency settings in `config.yaml` are appropriate for the hardware.

## Web API Not Responding
- Confirm the FastAPI service is running.
- Look for error messages in the application logs.
- Check that rate limiting or API key restrictions are not blocking requests.

## Failed Deployment
- Review the CI logs for test or security scan failures.
- Ensure Kubernetes manifests reference the correct image tag.

## Data Synchronization Issues
- Verify scheduled jobs that move data between staging and production.
- Check replication lag metrics on the database servers.
