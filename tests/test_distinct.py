import pytest

from pysqlscribe.query import Query
from pysqlscribe.table import Table


@pytest.mark.parametrize(
    "dialect,expected",
    [
        ("postgres", 'SELECT DISTINCT "test_column" FROM "test_table"'),
        ("mysql", "SELECT DISTINCT `test_column` FROM `test_table`"),
        ("sqlite", 'SELECT DISTINCT "test_column" FROM "test_table"'),
        ("oracle", 'SELECT DISTINCT "test_column" FROM "test_table"'),
    ],
)
def test_select_distinct_per_dialect(dialect, expected):
    query = (
        Query(dialect).select("test_column", distinct=True).from_("test_table").build()
    )
    assert query == expected


def test_select_distinct_multiple_columns():
    query = (
        Query("postgres")
        .select("first_name", "last_name", distinct=True)
        .from_("employees")
        .build()
    )
    assert query == 'SELECT DISTINCT "first_name", "last_name" FROM "employees"'


def test_select_distinct_default_false_emits_plain_select():
    query = Query("postgres").select("test_column").from_("test_table").build()
    assert query == 'SELECT "test_column" FROM "test_table"'


def test_select_distinct_explicit_false_emits_plain_select():
    query = (
        Query("postgres")
        .select("test_column", distinct=False)
        .from_("test_table")
        .build()
    )
    assert query == 'SELECT "test_column" FROM "test_table"'


def test_select_distinct_with_where():
    query = (
        Query("postgres")
        .select("department", distinct=True)
        .from_("employees")
        .where("salary > 1000")
        .build()
    )
    assert query == 'SELECT DISTINCT "department" FROM "employees" WHERE salary > 1000'


def test_select_distinct_with_parameterize():
    table = Table("employees", "department", "salary", dialect="postgres")
    sql, params = (
        table.select("department", distinct=True)
        .where(table.salary > 1000)
        .build(parameterize=True)
    )
    assert sql == (
        'SELECT DISTINCT "department" FROM "employees" WHERE employees.salary > %s'
    )
    assert params == [1000]


def test_select_distinct_no_columns_emits_distinct_star():
    # `select(distinct=True)` falls back to `*`. Standard SQL treats
    # `SELECT DISTINCT *` as valid (and a no-op when the table has a key),
    # so we just emit it rather than erroring.
    query = Query("postgres").select(distinct=True).from_("test_table").build()
    assert query == 'SELECT DISTINCT * FROM "test_table"'


def test_table_select_distinct():
    table = Table("employees", "department", dialect="mysql")
    query = table.select("department", distinct=True).build()
    assert query == "SELECT DISTINCT `department` FROM `employees`"


def test_table_select_distinct_with_column_object():
    table = Table("employees", "department", "salary", dialect="postgres")
    query = table.select(table.department, distinct=True).build()
    assert query == 'SELECT DISTINCT "department" FROM "employees"'
