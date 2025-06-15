from pathlib import Path

from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    sql_path = (
        Path(__file__).resolve().parents[2] / "sql" / "create_postgres_schema.sql"
    )
    op.execute(sql_path.read_text())


def downgrade():
    op.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
