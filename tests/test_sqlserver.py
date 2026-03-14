import pytest

from pysqlscribe.ast.joins import JoinType
from pysqlscribe.exceptions import InvalidJoinException
from pysqlscribe.query import Query
from pysqlscribe.table import Table


@pytest.mark.parametrize(
    "fields",
    [["test_column"], ["test_column", "another_test_column"]],
    ids=["single field", "multiple fields"],
)
def test_select_query(fields):
    query_builder = Query("sqlserver")
    query = query_builder.select(*fields).from_("test_table").build()
    escaped = [f"[{f}]" for f in fields]
    assert query == f"SELECT {', '.join(escaped)} FROM [test_table]"


def test_select_query_no_columns():
    query_builder = Query("sqlserver")
    query = query_builder.select().from_("test_table").build()
    assert query == "SELECT * FROM [test_table]"


def test_select_with_offset_and_limit():
    query_builder = Query("sqlserver")
    query = (
        query_builder.select("test_column")
        .from_("test_table")
        .order_by("test_column")
        .offset(5)
        .limit(10)
        .build()
    )
    assert (
        query
        == "SELECT [test_column] FROM [test_table] ORDER BY [test_column] OFFSET 5 ROWS FETCH NEXT 10 ROWS ONLY"
    )


def test_where_clause():
    query_builder = Query("sqlserver")
    query = (
        query_builder.select("test_column", "another_test_column")
        .from_("test_table")
        .where("test_column = 1", "another_test_column > 2")
        .build()
    )
    assert (
        query
        == "SELECT [test_column], [another_test_column] FROM [test_table] WHERE test_column = 1 AND another_test_column > 2"
    )


@pytest.mark.parametrize(
    "join_type", [JoinType.INNER, JoinType.OUTER, JoinType.LEFT, JoinType.RIGHT]
)
def test_joins_with_conditions(join_type: JoinType):
    query_builder = Query("sqlserver")
    query = (
        query_builder.select("employee_id", "store_location")
        .from_("employees")
        .join("payroll", join_type, "employees.payroll_id = payroll.id")
        .build()
    )
    assert (
        query
        == f"SELECT [employee_id], [store_location] FROM [employees] {join_type} JOIN [payroll] ON employees.payroll_id = payroll.id"
    )


def test_invalid_join():
    query_builder = Query("sqlserver")
    with pytest.raises(InvalidJoinException):
        query_builder.select("employee_id").from_("employees").join(
            "payroll", JoinType.NATURAL, "employees.payroll_id = payroll.id"
        )


def test_table_select():
    table = Table(
        "test_table", "test_column", "another_test_column", dialect="sqlserver"
    )
    query = table.select("test_column").build()
    assert query == "SELECT [test_column] FROM [test_table]"


def test_table_where_clause():
    table = Table("test_table", "test_column", dialect="sqlserver")
    query = table.select("test_column").where(table.test_column > 5).build()
    assert (
        query
        == "SELECT [test_column] FROM [test_table] WHERE test_table.test_column > 5"
    )


def test_insert():
    query_builder = Query("sqlserver")
    query = query_builder.insert(
        "test_column", "another_test_column", into="test_table", values=(1, 2)
    ).build()
    assert (
        query
        == "INSERT INTO [test_table] ([test_column], [another_test_column]) VALUES (1,2)"
    )
