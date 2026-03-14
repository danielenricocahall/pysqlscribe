import pytest

from pysqlscribe.ast.joins import JoinType
from pysqlscribe.table import Table


def test_alias():
    employee_table = Table(
        "employee", "first_name", "last_name", "dept", "payroll_id", dialect="postgres"
    )
    query = (
        employee_table.as_("e").select(employee_table.first_name.as_("name")).build()
    )
    assert query == 'SELECT "first_name" AS name FROM "employee" AS e'


def test_invalid_alias():
    employee_table = Table(
        "employee", "first_name", "last_name", "dept", "payroll_id", dialect="postgres"
    )
    with pytest.raises(ValueError):
        employee_table.as_("$something$not$allowed").select(
            employee_table.first_name.as_("name")
        ).build()


@pytest.mark.parametrize(
    "join_type", [JoinType.INNER, JoinType.OUTER, JoinType.LEFT, JoinType.RIGHT]
)
def test_table_join_with_alias(join_type: JoinType):
    employee_table = Table(
        "employee", "first_name", "last_name", "dept", "payroll_id", dialect="postgres"
    )
    payroll_table = Table("payroll", "id", "salary", "category", dialect="postgres")
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
        == f'SELECT "first_name", "last_name", "dept" FROM "employee" AS e {join_type} JOIN "payroll" AS p ON p.id = e.payroll_id WHERE p.salary > 1000'
    )
