from pysqlscribe.schema import Schema
from pysqlscribe.table import Table, OracleTable, PostgresTable, SqliteTable


def test_schema_create_tables():
    schema = Schema(
        "test_schema", ["test_table", "another_test_table"], dialect="postgres"
    )
    assert len(schema.tables) == 2
    assert all(isinstance(table, PostgresTable) for table in schema.tables)
    assert all(table.table_name.startswith("test_schema") for table in schema.tables)
    assert hasattr(schema, "test_table")


def test_schema_dialect_environment_provided(monkeypatch):
    monkeypatch.setenv("PYSQLSCRIBE_BUILDER_DIALECT", "sqlite")
    schema = Schema("test_schema", ["test_table", "another_test_table"])
    assert len(schema.tables) == 2
    assert all(isinstance(table, SqliteTable) for table in schema.tables)
    assert all(table.table_name.startswith("test_schema") for table in schema.tables)
    assert hasattr(schema, "test_table")


def test_schema_supply_tables():
    tables = [
        OracleTable("cards", "stars", "card_name", "card_description", "card_cost"),
        OracleTable("regions", "region_id", "region_name"),
    ]
    schema = Schema("kaibacorp", tables)
    assert len(schema.tables) == 2
    assert all(isinstance(table, Table) for table in schema.tables)
    assert all(table.table_name.startswith("kaibacorp") for table in schema.tables)
    assert hasattr(schema, "cards")
