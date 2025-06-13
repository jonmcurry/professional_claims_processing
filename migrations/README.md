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
