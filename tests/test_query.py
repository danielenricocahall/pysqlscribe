from itertools import product

import pytest
from pysqlscribe.query import (
    QueryRegistry,
    JoinType,
    InvalidJoinException,
    UNION,
    EXCEPT,
    INTERSECT,
)


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


def test_select_query_no_columns():
    query_builder = QueryRegistry.get_builder("mysql")
    query = query_builder.select().from_("test_table").build()
    assert query == "SELECT * FROM `test_table`"


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


@pytest.mark.parametrize("join_type", [JoinType.NATURAL, JoinType.CROSS])
def test_joins_no_condition(join_type: JoinType):
    query_builder = QueryRegistry.get_builder("oracle")
    query_builder.select("employee_id", "store_location").from_("employees").join(
        "payroll", join_type
    )
    query = query_builder.build()
    assert (
        query
        == f'SELECT "employee_id","store_location" FROM "employees" {join_type} JOIN "payroll"'
    )


def test_invalid_join():
    query_builder = QueryRegistry.get_builder("oracle")
    with pytest.raises(InvalidJoinException):
        query_builder.select("employee_id", "store_location").from_("employees").join(
            "payroll", JoinType.NATURAL, "employees.payroll_id = payroll.id"
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


def test_disable_escape_identifier():
    query_builder = QueryRegistry.get_builder("mysql").disable_escape_identifiers()
    query = (
        query_builder.select("test_column", "another_test_column")
        .from_("test_table")
        .where("test_column = 1", "another_test_column > 2")
        .build()
    )
    assert (
        query
        == "SELECT test_column,another_test_column FROM test_table WHERE test_column = 1 AND another_test_column > 2"
    )


def test_escape_identifier_switch_preferences():
    query_builder = QueryRegistry.get_builder("mysql").disable_escape_identifiers()
    query = (
        query_builder.select("test_column", "another_test_column")
        .enable_escape_identifiers()
        .from_("test_table")
        .where("test_column = 1", "another_test_column > 2")
        .build()
    )
    assert (
        query
        == "SELECT test_column,another_test_column FROM `test_table` WHERE test_column = 1 AND another_test_column > 2"
    )


def test_disable_escape_identifier_with_environment_variable(monkeypatch):
    monkeypatch.setenv("PYSQLSCRIBE_ESCAPE_IDENTIFIERS", "False")
    query_builder = QueryRegistry.get_builder("mysql")
    query = (
        query_builder.select("test_column", "another_test_column")
        .from_("test_table")
        .where("test_column = 1", "another_test_column > 2")
        .build()
    )
    assert (
        query
        == "SELECT test_column,another_test_column FROM test_table WHERE test_column = 1 AND another_test_column > 2"
    )


@pytest.mark.parametrize("all_", [True, False])
def test_union(all_):
    query_builder = QueryRegistry.get_builder("mysql")
    another_query_builder = QueryRegistry.get_builder("mysql")
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
    query_builder = QueryRegistry.get_builder("mysql")
    another_query_builder = QueryRegistry.get_builder("mysql")
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


def test_insert():
    query_builder = QueryRegistry.get_builder("mysql")
    query = query_builder.insert(
        "test_column",
        "another_test_column",
        into="test_table",
        values=(1, 2),
    ).build()
    assert (
        query
        == "INSERT INTO `test_table` (`test_column`,`another_test_column`) VALUES (1,2)"
    )


def test_insert_no_cols_query():
    query_builder = QueryRegistry.get_builder("mysql")
    query = query_builder.insert(
        into="test_table",
        values=(1, 2),
    ).build()
    assert query == "INSERT INTO `test_table` VALUES (1,2)"


def test_insert_multiple_values():
    query_builder = QueryRegistry.get_builder("mysql")
    query = query_builder.insert(
        "test_column", "another_test_column", into="test_table", values=[(1, 2), (3, 4)]
    ).build()
    assert (
        query
        == "INSERT INTO `test_table` (`test_column`,`another_test_column`) VALUES (1,2),(3,4)"
    )


def test_insert_column_and_values_mismatch():
    query_builder = QueryRegistry.get_builder("mysql")
    with pytest.raises(AssertionError):
        query_builder.insert(
            "test_column", "another_test_column", into="test_table", values=(1,)
        )


def test_insert_no_table_provided():
    query_builder = QueryRegistry.get_builder("mysql")
    with pytest.raises(ValueError):
        query_builder.insert("test_column", "another_test_column", values=(1, 2))


@pytest.mark.parametrize("return_value", ["id", "*"])
def test_insert_with_returning(return_value):
    query_builder = QueryRegistry.get_builder("postgres")
    query = (
        query_builder.insert(
            "id", "employee_name", into="employees", values=(1, "'john doe'")
        )
        .returning(return_value)
        .build()
    )
    if return_value != "*":
        return_value = query_builder.escape_identifier(return_value)
    assert (
        query
        == f'INSERT INTO "employees" ("id","employee_name") VALUES (1,\'john doe\') RETURNING {return_value}'
    )


def test_insert_with_returning_multiple_values():
    query_builder = QueryRegistry.get_builder("postgres")
    query = (
        query_builder.insert(
            "id", "employee_name", into="employees", values=(1, "'john doe'")
        )
        .returning("id", "employee_name")
        .build()
    )
    assert (
        query
        == 'INSERT INTO "employees" ("id","employee_name") VALUES (1,\'john doe\') RETURNING "id","employee_name"'
    )


def test_insert_returning_empty():
    query_builder = QueryRegistry.get_builder("postgres")
    query = (
        query_builder.insert(
            "id", "employee_name", into="employees", values=(1, "'john doe'")
        )
        .returning()
        .build()
    )
    assert (
        query
        == 'INSERT INTO "employees" ("id","employee_name") VALUES (1,\'john doe\') RETURNING *'
    )


def test_where_clause_with_subquery():
    subquery_builder = QueryRegistry.get_builder("mysql")
    subquery = (
        subquery_builder.select("id").from_("employees").where("salary > 10000").build()
    )

    query_builder = QueryRegistry.get_builder("mysql")
    query = (
        query_builder.select("employee_name", "salary")
        .from_("employees")
        .where(f"id IN ({subquery})", "department = 'Engineering'")
        .build()
    )
    assert (
        query
        == "SELECT `employee_name`,`salary` FROM `employees` WHERE id IN (SELECT `id` FROM `employees` WHERE salary > 10000) AND department = 'Engineering'"
    )


def test_subquery():

    query_builder = QueryRegistry.get_builder("mysql")
    another_query_builder = QueryRegistry.get_builder("mysql")
    subquery = (
        query_builder.select("test_column", "another_test_column")
        .from_("test_table")
        .where("test_column = 1")
    )
    query = (
        another_query_builder.select("test_column")
        .from_(subquery)
        .where("another_test_column > 2")
        .build()
    )
    assert (
        query
        == "SELECT `test_column` FROM (SELECT `test_column`,`another_test_column` FROM `test_table` WHERE test_column = 1) WHERE another_test_column > 2"
    )
