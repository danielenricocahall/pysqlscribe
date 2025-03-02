import pytest

from pysqlscribe.query import JoinType

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


def test_column_operations():
    table = MySQLTable("employees", "salary", "bonus", "lti")
    query = table.select((table.salary * 0.75).as_("salary_after_taxes")).build()
    assert (
        query == "SELECT employees.salary * 0.75 AS salary_after_taxes FROM `employees`"
    )


def test_add_columns():
    table = MySQLTable("employees", "salary", "bonus", "lti")
    query = table.select((table.salary + table.bonus).as_("total_compensation")).build()
    assert (
        query
        == "SELECT employees.salary + employees.bonus AS total_compensation FROM `employees`"
    )
