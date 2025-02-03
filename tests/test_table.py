import pytest

from pyquerybuilder.table import Table, InvalidFieldsException


def test_table_select():
    table = Table("test_table", "test_field", "another_test_field")
    query = table.select("test_field").build()
    assert query == "SELECT test_field FROM test_table"


def test_table_non_existent_field():
    table = Table("test_table", "test_field", "another_test_field")
    with pytest.raises(InvalidFieldsException):
        table.select("some_nonexistent_test_field")
