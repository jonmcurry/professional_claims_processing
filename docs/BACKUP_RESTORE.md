# Backup and Restore Procedures

Schedule periodic backups of the PostgreSQL and SQL Server databases. Backups can be performed with `pg_dump` and `sqlcmd` or `sqlpackage` depending on your environment.

Example backup script:

```bash
python src/maintenance/backup_restore.py backup
```

Verify backups by performing a test restore periodically:

```bash
python src/maintenance/backup_restore.py restore path/to/backup.sql
```
