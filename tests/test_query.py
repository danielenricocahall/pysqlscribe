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


@pytest.mark.parametrize(
    "dialect,syntax",
    [("mysql", "LIMIT {limit}"), ("oracle", "FETCH NEXT {limit} ROWS ONLY")],
)
def test_select_query_with_limit(dialect, syntax):
    query_builder = QueryRegistry.get_builder(dialect)
    query = query_builder.select("test_field").from_("test_table").limit(10).build()
    assert query == f"SELECT test_field FROM test_table {syntax.format(limit=10)}"


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


def test_where_clause():
    query_builder = QueryRegistry.get_builder("mysql")
    query = (
        query_builder.select("test_field", "another_test_field")
        .from_("test_table")
        .where("test_field = 1", "another_test_field > 2")
        .build()
    )
    assert (
        query
        == "SELECT test_field,another_test_field FROM test_table WHERE test_field = 1 AND another_test_field > 2"
    )
