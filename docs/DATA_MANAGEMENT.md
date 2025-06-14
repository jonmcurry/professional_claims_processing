# Data Management

## Data Archival Strategy for Old Claims
To keep the primary databases lean, claim records are retained in the active tables for 36 months. Older records are archived to long‑term storage.

## Table Partitioning
The `partition_historical_data.py` helper creates yearly partitions of the `claims`
table so that lookups on recent data remain fast while older data is isolated.
Run the script periodically to create new partitions for the current year:

```bash
python -m src.maintenance.partition_historical_data
```

### Archival Process
1. A nightly maintenance job runs `python -m src.maintenance.archive_old_claims`.
2. Claims where `service_to_date` is more than 36 months old are exported to encrypted JSON Lines files under `archive/YYYY/MM/`. Each record is first processed with `encrypt_claim_fields` and then the entire JSON string is encrypted with `encrypt_text`.
3. After export, the archived rows are deleted from the live tables in a single transaction.
4. The archived files are uploaded to cold storage (for example, an S3 bucket or on‑prem object store).

The encryption key used for the archive is the same `encryption_key` from `config.yaml`.

### Restoration
Each line in the archive must be decrypted with `decrypt_text` using the same `encryption_key`. The resulting JSON contains field‑level encrypted values which should be decrypted again before insertion. See the `restore_archived_claims.py` helper for a full example.

## Automated Retention Policy
The `enforce_retention_policy.py` helper under `src/maintenance` can be scheduled via cron to automatically archive claims older than 36 months and purge archive files beyond the seven‑year retention window.

Example cron entry:
```
0 2 * * * python -m src.maintenance.enforce_retention_policy
```
This job keeps production tables slim while ensuring long‑term records are retained for regulatory purposes.
