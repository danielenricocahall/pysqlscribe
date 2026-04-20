from itertools import product

import pytest

from pysqlscribe.aggregate_functions import avg
from pysqlscribe.exceptions import InvalidJoinError, InvalidNodeError
from pysqlscribe.ast.joins import JoinType
from pysqlscribe.query import Query
from pysqlscribe.renderers.base import UNION, EXCEPT, INTERSECT


@pytest.mark.parametrize(
    "fields",
    [["test_column"], ["test_column", "another_test_column"]],
    ids=["single field", "multiple fields"],
)
def test_select_query(fields):
    query_builder = Query("mysql")
    query = query_builder.select(*fields).from_("test_table").build()
    fields = [
        query_builder.dialect.escape_identifier(identifier) for identifier in fields
    ]
    assert query == f"SELECT {', '.join(fields)} FROM `test_table`"


def test_select_query_no_columns():
    query_builder = Query("mysql")
    query = query_builder.select().from_("test_table").build()
    assert query == "SELECT * FROM `test_table`"


@pytest.mark.parametrize(
    "dialect,syntax",
    [("mysql", "LIMIT {limit}"), ("oracle", "FETCH NEXT {limit} ROWS ONLY")],
)
def test_select_query_with_limit(dialect, syntax):
    query_builder = Query(dialect)
    query = query_builder.select("test_column").from_("test_table").limit(10).build()
    assert (
        query
        == f"SELECT {query_builder.dialect.escape_identifier('test_column')} FROM {query_builder.dialect.escape_identifier('test_table')} {syntax.format(limit=10)}"
    )


def test_select_query_with_limit_and_offset():
    query_builder = Query("postgres")
    query = (
        query_builder.select("test_column")
        .from_("test_table")
        .limit(10)
        .offset(5)
        .build()
    )
    assert (
        query
        == f"SELECT {query_builder.dialect.escape_identifier('test_column')} FROM {query_builder.dialect.escape_identifier('test_table')} LIMIT 10 OFFSET 5"
    )


def test_select_query_with_order_by():
    query_builder = Query("mysql")
    query = (
        query_builder.select("test_column", "another_test_column")
        .from_("test_table")
        .order_by("test_column")
        .build()
    )
    assert (
        query
        == "SELECT `test_column`, `another_test_column` FROM `test_table` ORDER BY `test_column`"
    )


def test_select_query_with_order_by_multiple_columns():
    query_builder = Query("mysql")
    query = (
        query_builder.select("test_column", "another_test_column")
        .from_("test_table")
        .order_by("test_column", "another_test_column")
        .build()
    )
    assert (
        query
        == "SELECT `test_column`, `another_test_column` FROM `test_table` ORDER BY `test_column`, `another_test_column`"
    )


def test_where_clause():
    query_builder = Query("mysql")
    query = (
        query_builder.select("test_column", "another_test_column")
        .from_("test_table")
        .where("test_column = 1", "another_test_column > 2")
        .build()
    )
    assert (
        query
        == "SELECT `test_column`, `another_test_column` FROM `test_table` WHERE test_column = 1 AND another_test_column > 2"
    )


def test_group_by_having():
    query_builder = Query("sqlite")
    query = (
        query_builder.select(
            "product_line", "AVG(unit_price)", "SUM(quantity)", "SUM(total)"
        )
        .from_("sales")
        .group_by("product_line")
        .having("SUM(total) > 1000")
        .build()
    )
    assert (
        query
        == 'SELECT "product_line", AVG(unit_price), SUM(quantity), SUM(total) FROM "sales" GROUP BY "product_line" HAVING SUM(total) > 1000'
    )


@pytest.mark.parametrize(
    "join_type", [JoinType.INNER, JoinType.OUTER, JoinType.LEFT, JoinType.RIGHT]
)
def test_joins_with_conditions(join_type: JoinType):
    query_builder = Query("oracle")
    query_builder.select("employee_id", "store_location").from_("employees").join(
        "payroll", join_type, "employees.payroll_id = payroll.id"
    )
    query = query_builder.build()
    assert (
        query
        == f'SELECT "employee_id", "store_location" FROM "employees" {join_type} JOIN "payroll" ON employees.payroll_id = payroll.id'
    )


@pytest.mark.parametrize("join_type", [JoinType.NATURAL, JoinType.CROSS])
def test_joins_no_condition(join_type: JoinType):
    query_builder = Query("oracle")
    query_builder.select("employee_id", "store_location").from_("employees").join(
        "payroll", join_type
    )
    query = query_builder.build()
    assert (
        query
        == f'SELECT "employee_id", "store_location" FROM "employees" {join_type} JOIN "payroll"'
    )


def test_invalid_join():
    query_builder = Query("oracle")
    with pytest.raises(InvalidJoinError):
        query_builder.select("employee_id", "store_location").from_("employees").join(
            "payroll", JoinType.NATURAL, "employees.payroll_id = payroll.id"
        )


@pytest.mark.parametrize(
    "join_type", [JoinType.INNER, JoinType.OUTER, JoinType.LEFT, JoinType.RIGHT]
)
def test_join_where_with_conditions(join_type: JoinType):
    query_builder = Query("oracle")
    query_builder.select("employee_id", "store_location").from_("employees").join(
        "payroll", join_type, "employees.payroll_id = payroll.id"
    ).where("employee.salary > 10000")
    query = query_builder.build()
    assert (
        query
        == f'SELECT "employee_id", "store_location" FROM "employees" {join_type} JOIN "payroll" ON employees.payroll_id = payroll.id WHERE employee.salary > 10000'
    )


def test_disable_escape_identifier():
    query_builder = Query("mysql").disable_escape_identifiers()
    query = (
        query_builder.select("test_column", "another_test_column")
        .from_("test_table")
        .where("test_column = 1", "another_test_column > 2")
        .build()
    )
    assert (
        query
        == "SELECT test_column, another_test_column FROM test_table WHERE test_column = 1 AND another_test_column > 2"
    )


def test_escape_identifier_switch_preferences():
    query_builder = Query("mysql").disable_escape_identifiers()
    query = (
        query_builder.select("test_column", "another_test_column")
        .enable_escape_identifiers()
        .from_("test_table")
        .where("test_column = 1", "another_test_column > 2")
        .build()
    )
    assert (
        query
        == "SELECT `test_column`, `another_test_column` FROM `test_table` WHERE test_column = 1 AND another_test_column > 2"
    )


def test_disable_escape_identifier_with_environment_variable(monkeypatch):
    monkeypatch.setenv("PYSQLSCRIBE_ESCAPE_IDENTIFIERS", "False")
    query_builder = Query("mysql")
    query = (
        query_builder.select("test_column", "another_test_column")
        .from_("test_table")
        .where("test_column = 1", "another_test_column > 2")
        .build()
    )
    assert (
        query
        == "SELECT test_column, another_test_column FROM test_table WHERE test_column = 1 AND another_test_column > 2"
    )


@pytest.mark.parametrize("all_", [True, False])
def test_union(all_):
    query_builder = Query("mysql")
    another_query_builder = Query("mysql")
    query = (
        query_builder.select("test_column")
        .from_("test_table")
        .union(
            another_query_builder.select("another_test_column").from_(
                "another_test_table"
            ),
            all_=all_,
        )
        .build()
    )
    union_str = "UNION ALL" if all_ else "UNION"
    assert (
        query
        == f"SELECT `test_column` FROM `test_table` {union_str} SELECT `another_test_column` FROM `another_test_table`"
    )


@pytest.mark.parametrize(
    "all_, combine_operation", product([True, False], [UNION, EXCEPT, INTERSECT])
)
def test_combine_operations(all_, combine_operation):
    query_builder = Query("mysql")
    another_query_builder = Query("mysql")
    query = query_builder.select("test_column").from_("test_table")
    another_query = another_query_builder.select("another_test_column").from_(
        "another_test_table"
    )
    if combine_operation == UNION:
        query = query.union(another_query, all_=all_)
    elif combine_operation == EXCEPT:
        query = query.except_(another_query, all_=all_)
    elif combine_operation == INTERSECT:
        query = query.intersect(another_query, all_=all_)

    merge_operation_str = f"{combine_operation} ALL" if all_ else f"{combine_operation}"
    assert (
        query.build()
        == f"SELECT `test_column` FROM `test_table` {merge_operation_str} SELECT `another_test_column` FROM `another_test_table`"
    )


def test_invalid_dialect():
    with pytest.raises(ValueError):
        Query("non-existent-dialect")


@pytest.mark.parametrize("dialect", ["mysql", "sqlite"])
def test_bare_offset_after_from_rejected(dialect):
    with pytest.raises(InvalidNodeError):
        Query(dialect).select("id").from_("t").offset(5).build()


@pytest.mark.parametrize("dialect", ["mysql", "sqlite"])
def test_bare_offset_after_union_rejected(dialect):
    a = Query(dialect).select("id").from_("a")
    b = Query(dialect).select("id").from_("b")
    with pytest.raises(InvalidNodeError):
        a.union(b).offset(5).build()


def test_postgres_bare_offset_after_from():
    query = Query("postgres").select("id").from_("t").offset(5).build()
    assert query == 'SELECT "id" FROM "t" OFFSET 5'


def test_postgres_bare_offset_after_union():
    a = Query("postgres").select("id").from_("a")
    b = Query("postgres").select("id").from_("b")
    query = a.union(b).offset(5).build()
    assert query == 'SELECT "id" FROM "a" UNION SELECT "id" FROM "b" OFFSET 5'


@pytest.mark.parametrize("dialect", ["mysql", "sqlite", "postgres"])
def test_post_union_order_limit_offset(dialect):
    a = Query(dialect).select("id").from_("a")
    b = Query(dialect).select("id").from_("b")
    query = a.union(b).order_by("id").limit(10).offset(5).build()
    esc = Query(dialect).dialect.escape_identifier
    assert query == (
        f"SELECT {esc('id')} FROM {esc('a')} "
        f"UNION SELECT {esc('id')} FROM {esc('b')} "
        f"ORDER BY {esc('id')} LIMIT 10 OFFSET 5"
    )


@pytest.mark.parametrize("dialect", ["mysql", "sqlite", "postgres"])
def test_chained_union(dialect):
    a = Query(dialect).select("id").from_("a")
    b = Query(dialect).select("id").from_("b")
    c = Query(dialect).select("id").from_("c")
    query = a.union(b).union(c).build()
    esc = Query(dialect).dialect.escape_identifier
    assert query == (
        f"SELECT {esc('id')} FROM {esc('a')} "
        f"UNION SELECT {esc('id')} FROM {esc('b')} "
        f"UNION SELECT {esc('id')} FROM {esc('c')}"
    )


def test_chained_where_merges_with_and():
    query = (
        Query("postgres")
        .select("x")
        .from_("t")
        .where("a = 1")
        .where("b = 2")
        .where("c = 3")
        .build()
    )
    assert query == 'SELECT "x" FROM "t" WHERE a = 1 AND b = 2 AND c = 3'


def test_chained_having_merges_with_and():
    query = (
        Query("postgres")
        .select("x")
        .from_("t")
        .group_by("x")
        .having("a = 1")
        .having("b = 2")
        .build()
    )
    assert query == 'SELECT "x" FROM "t" GROUP BY "x" HAVING a = 1 AND b = 2'


def test_oracle_order_by_limit_without_offset():
    query = Query("oracle").select("x").from_("t").order_by("x").limit(10).build()
    assert query == 'SELECT "x" FROM "t" ORDER BY "x" FETCH NEXT 10 ROWS ONLY'


def test_oracle_offset_limit_without_order_by():
    query = Query("oracle").select("x").from_("t").offset(5).limit(10).build()
    assert query == 'SELECT "x" FROM "t" OFFSET 5 ROWS FETCH NEXT 10 ROWS ONLY'


def test_oracle_rejects_nodes_after_limit():
    with pytest.raises(InvalidNodeError):
        Query("oracle").select("x").from_("t").limit(10).order_by("x").build()


def test_subquery():
    query_builder = Query("mysql")
    query_builder.select("department", avg("salary")).from_("employees").group_by(
        "department"
    )
    another_query_builder = Query("mysql")
    query = (
        another_query_builder.select("*")
        .from_(query_builder.as_("aggregated_employees"))
        .build()
    )
    assert (
        query
        == "SELECT * FROM (SELECT `department`, AVG(salary) FROM `employees` GROUP BY `department`) AS aggregated_employees"
    )


def test_raw_string_alias_rejects_injection():
    with pytest.raises(ValueError, match="Invalid SQL alias"):
        Query("postgres").select("col AS bad; DROP TABLE users").from_("t").build()


def test_raw_string_alias_accepts_valid():
    query = Query("postgres").select("col AS total").from_("t").build()
    assert query == 'SELECT "col" AS total FROM "t"'
