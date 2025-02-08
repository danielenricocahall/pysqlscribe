import pytest

from pysqlscribe.table import InvalidColumnsException, MySQLTable, OracleTable, Table


def test_table_select():
    table = MySQLTable("test_table", "test_column", "another_test_column")
    query = table.select("test_column").build()
    assert query == "SELECT `test_column` FROM `test_table`"
    assert hasattr(table, "test_column")
    assert table.columns == ("test_column", "another_test_column")


def test_table_non_existent_field():
    table = MySQLTable("test_table", "test_column", "another_test_column")
    with pytest.raises(InvalidColumnsException):
        table.select("some_nonexistent_test_column")


def test_table_with_schema():
    table = MySQLTable(
        "test_table", "test_column", "another_test_column", schema="test_schema"
    )
    query = table.select("test_column", "another_test_column").build()
    assert (
        query
        == "SELECT `test_column`,`another_test_column` FROM `test_schema.test_table`"
    )
    assert table.name == "test_schema.test_table"


def test_create_existing_table_type():
    oracle_table_class = Table.create("oracle")
    assert oracle_table_class == OracleTable


def test_create_invalid_dialect():
    with pytest.raises(ValueError):
        Table.create("non-existent-dialect")


def test_table_reassign_columns():
    old_columns = ["employees", "locations"]
    table = OracleTable("capsule_corp", *old_columns)
    new_columns = ["peons", "orders", "suppliers", "regions"]
    table.columns = new_columns
    assert all(hasattr(table, column) for column in new_columns)
    assert all(not hasattr(table, column) for column in old_columns)
