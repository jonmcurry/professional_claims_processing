# Backup and Restore Procedures

Schedule periodic backups of the PostgreSQL and SQL Server databases. Backups can be performed with `pg_dump` and `sqlcmd` or `sqlpackage` depending on your environment.

Backups are encrypted using GnuPG with the `security.encryption_key` from
`config.yaml`.  The backup file is written with a `.sql.gpg` extension.

Example backup script:

```bash
python src/maintenance/backup_restore.py backup
```

Verify backups by performing a test restore periodically:

```bash
python src/maintenance/backup_restore.py restore path/to/backup.sql.gpg
```

The same encryption key must be present in `config.yaml` when restoring.
