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

## Automatic Recovery
The processing pipeline runs a **recovery manager** in the background. It watches
for degraded or backup mode and will:

- Reconnect to databases and Redis
- Warm the RVU cache
- Flush any claims stored on disk while in backup mode

If recovery fails three times in a row an email notification is sent to the
recipients configured under `monitoring.alerts.email_recipients`.

### Manual Override
You can trigger a recovery attempt or flush the queued claims manually:

```bash
python src/maintenance/recovery_manager.py         # attempt recovery once
python src/maintenance/recovery_manager.py flush   # just flush the local queue
```
