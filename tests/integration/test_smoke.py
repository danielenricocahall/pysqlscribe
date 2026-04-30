"""End-to-end smoke tests: build a pysqlscribe query, execute against a real
driver, assert the rows come back. Catches placeholder-format and binding
regressions that pure-string assertions can't see.
"""

import datetime

import pytest

from pysqlscribe.table import Table


pytestmark = pytest.mark.integration


def test_sqlite_inline_build_executes(sqlite_conn):
    employees = Table("employees", "id", "name", "salary", dialect="sqlite")
    sql = employees.select("name").where(employees.salary > 150).build()
    rows = sqlite_conn.execute(sql).fetchall()
    assert sorted(name for (name,) in rows) == ["Bob", "Carol"]


def test_sqlite_inline_build_with_datetime_literal_executes(sqlite_conn):
    """Round-trip a datetime literal through the inline build path: proves the
    rendered `'YYYY-MM-DD HH:MM:SS'` string is something a real driver accepts.
    """
    events = Table("events", "id", "created_at", dialect="sqlite")
    sql = (
        events.select("id")
        .where(events.created_at == datetime.datetime(2026, 4, 28, 14, 30, 0))
        .build()
    )
    rows = sqlite_conn.execute(sql).fetchall()
    assert [row_id for (row_id,) in rows] == [2]


def test_postgres_inline_build_executes(postgres_conn):
    employees = Table("employees", "id", "name", "salary", dialect="postgres")
    sql = employees.select("name").where(employees.salary > 150).build()
    with postgres_conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
    assert sorted(name for (name,) in rows) == ["Bob", "Carol"]


def test_sqlite_parameterized_where_roundtrip(sqlite_conn):
    employees = Table("employees", "id", "name", "salary", dialect="sqlite")
    sql, params = (
        employees.select("name").where(employees.salary > 150).build(parameterize=True)
    )
    assert "?" in sql
    rows = sqlite_conn.execute(sql, params).fetchall()
    assert sorted(name for (name,) in rows) == ["Bob", "Carol"]


def test_sqlite_parameterized_in_clause_roundtrip(sqlite_conn):
    employees = Table("employees", "id", "name", "salary", dialect="sqlite")
    sql, params = (
        employees.select("name")
        .where(employees.id.in_([1, 3]))
        .build(parameterize=True)
    )
    assert sql.count("?") == 2
    rows = sqlite_conn.execute(sql, params).fetchall()
    assert sorted(name for (name,) in rows) == ["Alice", "Carol"]


def test_sqlite_parameterized_between_roundtrip(sqlite_conn):
    employees = Table("employees", "id", "name", "salary", dialect="sqlite")
    sql, params = (
        employees.select("name")
        .where(employees.salary.between(150, 250))
        .build(parameterize=True)
    )
    rows = sqlite_conn.execute(sql, params).fetchall()
    assert [name for (name,) in rows] == ["Bob"]


def test_postgres_parameterized_where_roundtrip(postgres_conn):
    employees = Table("employees", "id", "name", "salary", dialect="postgres")
    sql, params = (
        employees.select("name").where(employees.salary > 150).build(parameterize=True)
    )
    assert "%s" in sql
    with postgres_conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    assert sorted(name for (name,) in rows) == ["Bob", "Carol"]


def test_postgres_parameterized_in_clause_roundtrip(postgres_conn):
    employees = Table("employees", "id", "name", "salary", dialect="postgres")
    sql, params = (
        employees.select("name")
        .where(employees.id.in_([1, 3]))
        .build(parameterize=True)
    )
    assert sql.count("%s") == 2
    with postgres_conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    assert sorted(name for (name,) in rows) == ["Alice", "Carol"]


def test_postgres_parameterized_between_roundtrip(postgres_conn):
    employees = Table("employees", "id", "name", "salary", dialect="postgres")
    sql, params = (
        employees.select("name")
        .where(employees.salary.between(150, 250))
        .build(parameterize=True)
    )
    with postgres_conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    assert [name for (name,) in rows] == ["Bob"]
