from pyquerybuilder.query import QueryRegistry


def test_simple_select_query():
    query_builder = QueryRegistry.get_builder("mysql")
    query = query_builder.select("test_field").from_("test_table").build()
    assert query == "SELECT test_field FROM test_table"
