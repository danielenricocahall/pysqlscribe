import pytest
from pyquerybuilder.query import QueryRegistry


@pytest.mark.parametrize(
    "fields",
    [["test_field"], ["test_field", "another_test_field"]],
    ids=["single field", "multiple fields"],
)
def test_select_query(fields):
    query_builder = QueryRegistry.get_builder("mysql")
    query = query_builder.select(*fields).from_("test_table").build()
    assert query == f"SELECT {','.join(fields)} FROM test_table"


def test_select_query_with_limit():
    query_builder = QueryRegistry.get_builder("mysql")
    query = query_builder.select("test_field").from_("test_table").limit(10).build()
    assert query == "SELECT test_field FROM test_table LIMIT 10"


def test_select_query_with_order_by():
    query_builder = QueryRegistry.get_builder("mysql")
    query = (
        query_builder.select("test_field", "another_test_field")
        .from_("test_table")
        .order_by("test_field")
        .build()
    )
    assert (
        query
        == "SELECT test_field,another_test_field FROM test_table ORDER BY test_field"
    )
