from pyquerybuilder.schema import Schema
from pyquerybuilder.table import Table, OracleTable


def test_schema_create_tables():
    schema = Schema("test_schema", ["test_table", "another_test_table"])
    assert len(schema.tables) == 2
    assert all(isinstance(table, Table) for table in schema.tables)
    assert all(table.name.startswith("test_schema") for table in schema.tables)
    assert hasattr(schema, "test_table")


def test_schema_supply_tables():
    tables = [
        OracleTable("cards", "stars", "card_name", "card_description", "card_cost"),
        OracleTable("regions", "region_id", "region_name"),
    ]
    schema = Schema("kaibacorp", tables)
    assert len(schema.tables) == 2
    assert all(isinstance(table, Table) for table in schema.tables)
    assert all(table.name.startswith("kaibacorp") for table in schema.tables)
    assert hasattr(schema, "cards")
