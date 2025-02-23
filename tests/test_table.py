import pytest

from pysqlscribe.aggregate_functions import count, max_
from pysqlscribe.query import JoinType
from pysqlscribe.scalar_functions import (
    abs_,
    floor,
    ABS,
    FLOOR,
    sqrt,
    SQRT,
    ceil,
    CEIL,
    round_,
    ROUND,
    upper,
    UPPER,
    lower,
    LOWER,
)
from pysqlscribe.table import (
    MySQLTable,
    OracleTable,
    Table,
    PostgresTable,
)


def test_table_select():
    table = MySQLTable("test_table", "test_column", "another_test_column")
    query = table.select("test_column").build()
    assert query == "SELECT `test_column` FROM `test_table`"
    assert hasattr(table, "test_column")
    assert table.columns == ("test_column", "another_test_column")


def test_table_with_schema():
    table = MySQLTable(
        "test_table", "test_column", "another_test_column", schema="test_schema"
    )
    query = table.select("test_column", "another_test_column").build()
    assert (
        query
        == "SELECT `test_column`,`another_test_column` FROM `test_schema.test_table`"
    )
    assert table.table_name == "test_schema.test_table"


def test_create_existing_table_type():
    oracle_table_class = Table.create("oracle")
    assert oracle_table_class == OracleTable


def test_create_invalid_dialect():
    with pytest.raises(ValueError):
        Table.create("non-existent-dialect")


def test_table_reassign_columns():
    old_columns = ["employees", "locations"]
    table = OracleTable("capsule_corp", *old_columns)
    new_columns = ["peons", "orders", "suppliers", "regions"]
    table.columns = new_columns
    assert all(hasattr(table, column) for column in new_columns)
    assert all(not hasattr(table, column) for column in old_columns)


def test_table_where_clause_fixed_value():
    table = MySQLTable("test_table", "test_column")
    query = table.select("test_column").where(table.test_column > 5).build()
    assert (
        query
        == "SELECT `test_column` FROM `test_table` WHERE test_table.test_column > 5"
    )


def test_table_where_clause_other_column():
    table = MySQLTable("test_table", "test_column", "other_test_column")
    query = (
        table.select(table.test_column)
        .where(table.test_column > table.other_test_column)
        .build()
    )
    assert (
        query
        == "SELECT `test_column` FROM `test_table` WHERE test_table.test_column > test_table.other_test_column"
    )


@pytest.mark.parametrize("agg_column", [1, "first_name"])
def test_table_group_by(agg_column):
    table = PostgresTable(
        "employee", "first_name", "last_name", "store_location", "salary"
    )
    query = (
        table.select(table.store_location, count(agg_column))
        .group_by(table.store_location)
        .build()
    )
    assert (
        query
        == f'SELECT "store_location",COUNT({agg_column}) FROM "employee" GROUP BY "store_location"'
    )


def test_table_group_by_with_column_object():
    table = PostgresTable(
        "employee", "first_name", "last_name", "store_location", "salary"
    )
    query = (
        table.select(table.store_location, max_(table.salary))
        .group_by(table.store_location)
        .build()
    )
    assert (
        query
        == 'SELECT "store_location",MAX(salary) FROM "employee" GROUP BY "store_location"'
    )


def test_table_select_all():
    table = PostgresTable("employee", "first_name", "last_name", "dept", "salary")
    query = table.select("*").where(table.dept == "Sales").build()
    assert query == "SELECT * FROM \"employee\" WHERE employee.dept = 'Sales'"


@pytest.mark.parametrize(
    "join_type", [JoinType.INNER, JoinType.OUTER, JoinType.LEFT, JoinType.RIGHT]
)
def test_table_join_with_conditions(join_type: JoinType):
    employee_table = PostgresTable(
        "employee", "first_name", "last_name", "dept", "payroll_id"
    )
    payroll_table = PostgresTable("payroll", "id", "salary", "category")
    query = (
        employee_table.select(
            employee_table.first_name, employee_table.last_name, employee_table.dept
        )
        .join(payroll_table, join_type, payroll_table.id == employee_table.payroll_id)
        .build()
    )
    assert (
        query
        == f'SELECT "first_name","last_name","dept" FROM "employee" {join_type} JOIN "payroll" ON payroll.id = employee.payroll_id'
    )


@pytest.mark.parametrize(
    "join_type", [JoinType.INNER, JoinType.OUTER, JoinType.LEFT, JoinType.RIGHT]
)
def test_table_join_with_alias(join_type: JoinType):
    employee_table = PostgresTable(
        "employee", "first_name", "last_name", "dept", "payroll_id"
    )
    payroll_table = PostgresTable("payroll", "id", "salary", "category")
    query = (
        employee_table.as_("e")
        .select(
            employee_table.first_name, employee_table.last_name, employee_table.dept
        )
        .join(
            payroll_table.as_("p"),
            join_type,
            payroll_table.id == employee_table.payroll_id,
        )
        .where(payroll_table.salary > 1000)
        .build()
    )
    assert (
        query
        == f'SELECT "first_name","last_name","dept" FROM "employee" AS e {join_type} JOIN "payroll" AS p ON p.id = e.payroll_id WHERE p.salary > 1000'
    )


@pytest.mark.parametrize("join_type", [JoinType.NATURAL, JoinType.CROSS])
def test_table_join_without_conditions(join_type: JoinType):
    employee_table = PostgresTable(
        "employee", "first_name", "last_name", "dept", "payroll_id"
    )
    payroll_table = PostgresTable("payroll", "id", "salary", "category")
    query = (
        employee_table.select(
            employee_table.first_name, employee_table.last_name, employee_table.dept
        )
        .join(payroll_table, join_type)
        .build()
    )
    assert (
        query
        == f'SELECT "first_name","last_name","dept" FROM "employee" {join_type} JOIN "payroll"'
    )


@pytest.mark.parametrize(
    "scalar_function,str_function",
    [
        (abs_, ABS),
        (floor, FLOOR),
        (sqrt, SQRT),
        (ceil, CEIL),
        (round_, ROUND),
        (upper, UPPER),
        (lower, LOWER),
    ],
)
def test_scalar_functions(scalar_function, str_function):
    payroll_table = PostgresTable("payroll", "id", "salary", "category")
    query = payroll_table.select(scalar_function(payroll_table.salary)).build()
    assert query == f'SELECT {str_function}(salary) FROM "payroll"'


def test_aliases():
    employee_table = PostgresTable(
        "employee", "first_name", "last_name", "dept", "payroll_id"
    )
    query = (
        employee_table.as_("e").select(employee_table.first_name.as_("name")).build()
    )
    assert query == 'SELECT "first_name" AS name FROM "employee" AS e'
