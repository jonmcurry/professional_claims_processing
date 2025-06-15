# Operations Runbook

This runbook describes routine tasks for keeping the claims processing system healthy.

## Daily Checks
- Review application logs for errors or warnings.
- Verify database replication status and backup jobs.
- Confirm that the `/health` and `/readiness` endpoints return `200`.

## Weekly Maintenance
- Verify log rotation has archived files under `logs/`. The application now
  rotates `audit.log` and `analytics.log` automatically based on size.
- Inspect the `failed_claims` table for recurring errors.
- Update ML model metrics using `src/models/evaluate_model.py`.

## Emergency Procedures
- If the service is down, restart the worker with:
  ```bash
  systemctl restart claims-worker
  ```
- For persistent database issues, switch to the standby instance and update `config.yaml` accordingly.

## Monthly Tasks
- Review change management log for completed deployments.
- Rotate encryption keys if required by policy.

## Incident Response
- Declare an incident in the monitoring system and page on-call staff.
- Document the resolution steps in the incident tracker.
