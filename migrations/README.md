# Database Migrations

This project uses **Alembic** for schema migrations. Install dependencies:

```bash
pip install alembic
```

Create new revisions with:

```bash
alembic revision -m "description"
```

Apply migrations using:

```bash
alembic upgrade head
```

The configuration is provided in `alembic.ini` and migrations live under `migrations/versions`.

Always test migrations in a staging environment before deploying to production. Use the `src/db/migrate.py` script to apply migrations from CI pipelines.
