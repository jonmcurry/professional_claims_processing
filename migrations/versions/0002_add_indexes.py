from pathlib import Path

from alembic import op

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    sql_path = Path(__file__).resolve().parents[2] / "sql" / "create_indexes.sql"
    op.execute(sql_path.read_text())


def downgrade() -> None:
    pass
