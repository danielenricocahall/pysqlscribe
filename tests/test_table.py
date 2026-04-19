import pytest

from pysqlscribe.aggregate_functions import avg
from pysqlscribe.ast.joins import JoinType

from pysqlscribe.table import Table


def test_table_select():
    table = Table("test_table", "test_column", "another_test_column", dialect="mysql")
    query = table.select("test_column").build()
    assert query == "SELECT `test_column` FROM `test_table`"
    assert hasattr(table, "test_column")
    assert table.columns == ("test_column", "another_test_column")


def test_table_with_schema():
    table = Table(
        "test_table",
        "test_column",
        "another_test_column",
        dialect="mysql",
        schema="test_schema",
    )
    query = table.select("test_column", "another_test_column").build()
    assert (
        query
        == "SELECT `test_column`, `another_test_column` FROM `test_schema.test_table`"
    )
    assert table.table_name == "test_schema.test_table"


def test_create_invalid_dialect():
    with pytest.raises(ValueError):
        Table("test_table", dialect="non-existent-dialect")


def test_table_reassign_columns():
    old_columns = ["employees", "locations"]
    table = Table("capsule_corp", *old_columns, dialect="oracle")
    new_columns = ["peons", "orders", "suppliers", "regions"]
    table.columns = new_columns
    assert all(hasattr(table, column) for column in new_columns)
    assert all(not hasattr(table, column) for column in old_columns)


def test_table_where_clause_fixed_value():
    table = Table("test_table", "test_column", dialect="mysql")
    query = table.select("test_column").where(table.test_column > 5).build()
    assert (
        query
        == "SELECT `test_column` FROM `test_table` WHERE test_table.test_column > 5"
    )


def test_table_where_clause_other_column():
    table = Table("test_table", "test_column", "other_test_column", dialect="mysql")
    query = (
        table.select(table.test_column)
        .where(table.test_column > table.other_test_column)
        .build()
    )
    assert (
        query
        == "SELECT `test_column` FROM `test_table` WHERE test_table.test_column > test_table.other_test_column"
    )


def test_table_where_is_null():
    table = Table("test_table", "test_column", dialect="mysql")
    query = table.select("test_column").where(table.test_column.is_null()).build()
    assert (
        query
        == "SELECT `test_column` FROM `test_table` WHERE test_table.test_column IS NULL"
    )


def test_table_where_or_composition():
    table = Table("test_table", "test_column", dialect="mysql")
    query = (
        table.select("test_column")
        .where((table.test_column == 1) | (table.test_column == 2))
        .build()
    )
    assert (
        query == "SELECT `test_column` FROM `test_table` "
        "WHERE (test_table.test_column = 1) OR (test_table.test_column = 2)"
    )


def test_table_where_not_is_null():
    table = Table("test_table", "test_column", dialect="mysql")
    query = table.select("test_column").where(~table.test_column.is_null()).build()
    assert (
        query
        == "SELECT `test_column` FROM `test_table` WHERE NOT (test_table.test_column IS NULL)"
    )


def test_table_select_all():
    table = Table(
        "employee", "first_name", "last_name", "dept", "salary", dialect="postgres"
    )
    query = table.select("*").where(table.dept == "Sales").build()
    assert query == "SELECT * FROM \"employee\" WHERE employee.dept = 'Sales'"


@pytest.mark.parametrize(
    "join_type", [JoinType.INNER, JoinType.OUTER, JoinType.LEFT, JoinType.RIGHT]
)
def test_table_join_with_conditions(join_type: JoinType):
    employee_table = Table(
        "employee", "first_name", "last_name", "dept", "payroll_id", dialect="postgres"
    )
    payroll_table = Table("payroll", "id", "salary", "category", dialect="postgres")
    query = (
        employee_table.select(
            employee_table.first_name, employee_table.last_name, employee_table.dept
        )
        .join(payroll_table, join_type, payroll_table.id == employee_table.payroll_id)
        .build()
    )
    assert (
        query
        == f'SELECT "first_name", "last_name", "dept" FROM "employee" {join_type} JOIN "payroll" ON payroll.id = employee.payroll_id'
    )


@pytest.mark.parametrize("join_type", [JoinType.NATURAL, JoinType.CROSS])
def test_table_join_without_conditions(join_type: JoinType):
    employee_table = Table(
        "employee", "first_name", "last_name", "dept", "payroll_id", dialect="postgres"
    )
    payroll_table = Table("payroll", "id", "salary", "category", dialect="postgres")
    query = (
        employee_table.select(
            employee_table.first_name, employee_table.last_name, employee_table.dept
        )
        .join(payroll_table, join_type)
        .build()
    )
    assert (
        query
        == f'SELECT "first_name", "last_name", "dept" FROM "employee" {join_type} JOIN "payroll"'
    )


def test_column_operations():
    table = Table("employees", "salary", "bonus", "lti", dialect="mysql")
    query = table.select((table.salary * 0.75).as_("salary_after_taxes")).build()
    assert (
        query == "SELECT employees.salary * 0.75 AS salary_after_taxes FROM `employees`"
    )


def test_add_two_columns():
    table = Table("employees", "salary", "bonus", "lti", dialect="mysql")
    query = table.select((table.salary + table.bonus).as_("total_compensation")).build()
    assert (
        query
        == "SELECT employees.salary + employees.bonus AS total_compensation FROM `employees`"
    )


def test_add_more_than_two_columns():
    table = Table("employees", "salary", "bonus", "lti", dialect="mysql")
    query = table.select(
        (table.salary + table.bonus + table.lti).as_("total_compensation")
    ).build()
    assert (
        query
        == "SELECT employees.salary + employees.bonus + employees.lti AS total_compensation FROM `employees`"
    )


def test_operations_columns_and_numerics():
    table = Table("employees", "salary", "bonus", dialect="mysql")
    query = table.select(
        (table.salary * 1.25 + table.bonus).as_("total_compensation")
    ).build()
    assert (
        query
        == "SELECT employees.salary * 1.25 + employees.bonus AS total_compensation FROM `employees`"
    )


def test_insert():
    table = Table("employees", "salary", "bonus", dialect="mysql")
    query = table.insert(table.salary, table.bonus, values=(100, 200)).build()
    assert query == "INSERT INTO `employees` (`salary`, `bonus`) VALUES (100,200)"


def test_subquery_columns():
    employees = Table("employees", "salary", "bonus", "department_id", dialect="mysql")
    departments = Table("departments", "id", "name", "manager_id", dialect="mysql")
    subquery = departments.select("id").where(departments.name == "Engineering")
    query = employees.select().where(employees.department_id.in_(subquery)).build()
    assert (
        query
        == "SELECT * FROM `employees` WHERE employees.department_id IN (SELECT `id` FROM `departments` WHERE departments.name = 'Engineering')"
    )


def test_groupby_having():
    table = Table("employees", "salary", "department_id", dialect="mysql")
    query = (
        table.select(table.department_id, avg(table.salary).as_("avg_salary"))
        .group_by(table.department_id)
        .having(avg(table.salary) > 10000)
        .build()
    )
    assert (
        query
        == "SELECT `department_id`, AVG(salary) AS avg_salary FROM `employees` GROUP BY `department_id` HAVING AVG(salary) > 10000"
    )


def test_rename_table():
    table = Table("old_table_name", "column1", "column2", dialect="mysql")
    table.table_name = "new_table_name"
    query = table.select("column1", "column2").where(table.column1 > 1).build()
    assert (
        query
        == "SELECT `column1`, `column2` FROM `new_table_name` WHERE new_table_name.column1 > 1"
    )


def test_order_by_asc():
    table = Table("test_table", "cost", "name", dialect="sqlite")
    query = table.select("*").order_by(table.cost.asc()).build()
    assert query == 'SELECT * FROM "test_table" ORDER BY "cost" ASC'


def test_order_by_desc():
    table = Table("test_table", "cost", "name", dialect="sqlite")
    query = table.select("*").order_by(table.cost.desc()).build()
    assert query == 'SELECT * FROM "test_table" ORDER BY "cost" DESC'


def test_order_by_multiple_columns_mixed_direction():
    table = Table("test_table", "cost", "name", dialect="sqlite")
    query = table.select("*").order_by(table.cost.asc(), table.name.desc()).build()
    assert query == 'SELECT * FROM "test_table" ORDER BY "cost" ASC, "name" DESC'


def test_order_by_plain_string_unchanged():
    table = Table("test_table", "cost", "name", dialect="sqlite")
    query = table.select("*").order_by("cost").build()
    assert query == 'SELECT * FROM "test_table" ORDER BY "cost"'
