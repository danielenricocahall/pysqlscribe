import pytest

from pyquerybuilder.table import InvalidFieldsException, MySQLTable, OracleTable, Table


def test_table_select():
    table = MySQLTable("test_table", "test_field", "another_test_field")
    query = table.select("test_field").build()
    assert query == "SELECT `test_field` FROM `test_table`"
    assert hasattr(table, "test_field")
    assert table.fields == ("test_field", "another_test_field")


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
    assert table.name == "test_schema.test_table"


def test_create_existing_table_type():
    oracle_table_class = Table.create("oracle")
    assert oracle_table_class == OracleTable


def test_create_invalid_dialect():
    with pytest.raises(ValueError):
        Table.create("non-existent-dialect")


def test_table_reassign_fields():
    old_fields = ["employees", "locations"]
    table = OracleTable("capsule_corp", *old_fields)
    new_fields = ["peons", "orders", "suppliers", "regions"]
    table.fields = new_fields
    assert all(hasattr(table, field) for field in new_fields)
    assert all(not hasattr(table, field) for field in old_fields)
