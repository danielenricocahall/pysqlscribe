import pytest
from pysqlscribe.query import QueryRegistry, JoinType


@pytest.mark.parametrize(
    "fields",
    [["test_column"], ["test_column", "another_test_column"]],
    ids=["single field", "multiple fields"],
)
def test_select_query(fields):
    query_builder = QueryRegistry.get_builder("mysql")
    query = query_builder.select(*fields).from_("test_table").build()
    fields = [query_builder.escape_identifier(identifier) for identifier in fields]
    assert query == f"SELECT {','.join(fields)} FROM `test_table`"


@pytest.mark.parametrize(
    "dialect,syntax",
    [("mysql", "LIMIT {limit}"), ("oracle", "FETCH NEXT {limit} ROWS ONLY")],
)
def test_select_query_with_limit(dialect, syntax):
    query_builder = QueryRegistry.get_builder(dialect)
    query = query_builder.select("test_column").from_("test_table").limit(10).build()
    assert (
        query
        == f"SELECT {query_builder.escape_identifier('test_column')} FROM {query_builder.escape_identifier('test_table')} {syntax.format(limit=10)}"
    )


def test_select_query_with_limit_and_offset():
    query_builder = QueryRegistry.get_builder("postgres")
    query = (
        query_builder.select("test_column")
        .from_("test_table")
        .limit(10)
        .offset(5)
        .build()
    )
    assert (
        query
        == f"SELECT {query_builder.escape_identifier('test_column')} FROM {query_builder.escape_identifier('test_table')} LIMIT 10 OFFSET 5"
    )


def test_select_query_with_order_by():
    query_builder = QueryRegistry.get_builder("mysql")
    query = (
        query_builder.select("test_column", "another_test_column")
        .from_("test_table")
        .order_by("test_column")
        .build()
    )
    assert (
        query
        == "SELECT `test_column`,`another_test_column` FROM `test_table` ORDER BY `test_column`"
    )


def test_where_clause():
    query_builder = QueryRegistry.get_builder("mysql")
    query = (
        query_builder.select("test_column", "another_test_column")
        .from_("test_table")
        .where("test_column = 1", "another_test_column > 2")
        .build()
    )
    assert (
        query
        == "SELECT `test_column`,`another_test_column` FROM `test_table` WHERE test_column = 1 AND another_test_column > 2"
    )


def test_group_by_having():
    query_builder = QueryRegistry.get_builder("sqlite")
    query = (
        query_builder.select(
            "product_line", "AVG(unit_price)", "SUM(quantity)", "SUM(total)"
        )
        .from_("sales")
        .group_by("product_line")
        .build()
    )
    assert (
        query
        == 'SELECT "product_line",AVG(unit_price),SUM(quantity),SUM(total) FROM "sales" GROUP BY "product_line"'
    )


@pytest.mark.parametrize(
    "join_type", [JoinType.INNER, JoinType.OUTER, JoinType.LEFT, JoinType.RIGHT]
)
def test_joins_with_conditions(join_type: JoinType):
    query_builder = QueryRegistry.get_builder("oracle")
    query_builder.select("employee_id", "store_location").from_("employees").join(
        "payroll", join_type, "employees.payroll_id = payroll.id"
    )
    query = query_builder.build()
    assert (
        query
        == f'SELECT "employee_id","store_location" FROM "employees" {join_type} JOIN "payroll" ON employees.payroll_id = payroll.id'
    )


@pytest.mark.parametrize(
    "join_type", [JoinType.INNER, JoinType.OUTER, JoinType.LEFT, JoinType.RIGHT]
)
def test_join_where_with_conditions(join_type: JoinType):
    query_builder = QueryRegistry.get_builder("oracle")
    query_builder.select("employee_id", "store_location").from_("employees").join(
        "payroll", join_type, "employees.payroll_id = payroll.id"
    ).where("employee.salary > 10000")
    query = query_builder.build()
    assert (
        query
        == f'SELECT "employee_id","store_location" FROM "employees" {join_type} JOIN "payroll" ON employees.payroll_id = payroll.id WHERE employee.salary > 10000'
    )
