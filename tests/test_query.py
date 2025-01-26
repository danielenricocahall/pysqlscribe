from pyquerybuilder.query import Query, MySQLQueryBuilder


def test_query_builder():
    query = Query("mysql")
    query.select("test_field").from_("test_table")
    assert isinstance(query._builder, (MySQLQueryBuilder, ))