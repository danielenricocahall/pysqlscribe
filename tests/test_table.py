import pytest

from pyquerybuilder.table import InvalidFieldsException, MySQLTable, OracleTable, Table


def test_table_select():
    table = MySQLTable("test_table", "test_field", "another_test_field")
    # table = Table("test_table", "test_field", "another_test_field")
    query = table.select("test_field").build()
    assert query == "SELECT `test_field` FROM `test_table`"


def test_table_non_existent_field():
    table = MySQLTable("test_table", "test_field", "another_test_field")
    with pytest.raises(InvalidFieldsException):
        table.select("some_nonexistent_test_field")


def test_table_with_schema():
    table = MySQLTable(
        "test_table", "test_field", "another_test_field", schema="test_schema"
    )
    query = table.select("test_field", "another_test_field").build()
    assert (
        query
        == "SELECT `test_field`,`another_test_field` FROM `test_schema.test_table`"
    )


def test_create_existing_table_type():
    oracle_table_class = Table.create("oracle")
    assert oracle_table_class == OracleTable


def test_create_invalid_dialect():
    with pytest.raises(ValueError):
        Table.create("non-existent-dialect")
