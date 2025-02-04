from pyquerybuilder.schema import Schema
from pyquerybuilder.table import Table


def test_schema_create_tables():
    schema = Schema("test_schema", ["test_table", "another_test_table"])
    assert len(schema.tables) == 2
    assert all(isinstance(table, Table) for table in schema.tables)
    assert all(table.name.startswith("test_schema") for table in schema.tables)
