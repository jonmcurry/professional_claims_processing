import sys
from types import SimpleNamespace

# Provide a dummy pyodbc module so importing scripts.setup does not fail
sys.modules.setdefault(
    "pyodbc",
    SimpleNamespace(connect=lambda *a, **k: None, Connection=object)
)

from scripts.setup import DatabaseSetupOrchestrator


def test_split_sql_handles_dollar_quotes():
    orchestrator = DatabaseSetupOrchestrator(None)
    sql = (
        "CREATE OR REPLACE PROCEDURE archive_old_claims(cutoff_date DATE)\n"
        "LANGUAGE plpgsql\n"
        "AS $$\n"
        "BEGIN\n"
        "    INSERT INTO archived_claims SELECT * FROM claims WHERE service_to_date < cutoff_date;\n"
        "    DELETE FROM claims WHERE service_to_date < cutoff_date;\n"
        "END;\n"
        "$$;"
    )
    statements = orchestrator._split_sql_statements(sql)
    assert len(statements) == 1
    assert statements[0].startswith("CREATE OR REPLACE PROCEDURE")
    assert statements[0].endswith("$$;")
