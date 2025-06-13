# Data Management

## Data Archival Strategy for Old Claims
To keep the primary databases lean, claim records are retained in the active tables for 36 months. Older records are archived to long‑term storage.

### Archival Process
1. A nightly maintenance job runs `python -m src.maintenance.archive_old_claims`.
2. Claims where `service_to_date` is more than 36 months old are exported to encrypted JSON Lines files under `archive/YYYY/MM/`.
3. After export, the archived rows are deleted from the live tables in a single transaction.
4. The archived files are uploaded to cold storage (for example, an S3 bucket or on‑prem object store).

The encryption key used for the archive is the same `encryption_key` from `config.yaml`.

### Restoration
To restore data from an archive file, decrypt it and load the records back into the database using a script such as `restore_archived_claims.py`.

## Automated Retention Policy
The `enforce_retention_policy.py` helper under `src/maintenance` can be scheduled via cron to automatically archive claims older than 36 months and purge archive files beyond the seven‑year retention window.

Example cron entry:
```
0 2 * * * python -m src.maintenance.enforce_retention_policy
```
This job keeps production tables slim while ensuring long‑term records are retained for regulatory purposes.
