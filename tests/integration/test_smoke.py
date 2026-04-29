"""End-to-end smoke tests: build a pysqlscribe query, execute against a real
driver, assert the rows come back. Catches placeholder-format and binding
regressions that pure-string assertions can't see.
"""

import pytest

from pysqlscribe.table import Table


pytestmark = pytest.mark.integration


def test_sqlite_inline_build_executes(sqlite_conn):
    employees = Table("employees", "id", "name", "salary", dialect="sqlite")
    sql = employees.select("name").where(employees.salary > 150).build()
    rows = sqlite_conn.execute(sql).fetchall()
    assert sorted(name for (name,) in rows) == ["Bob", "Carol"]


def test_postgres_inline_build_executes(postgres_conn):
    employees = Table("employees", "id", "name", "salary", dialect="postgres")
    sql = employees.select("name").where(employees.salary > 150).build()
    with postgres_conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
    assert sorted(name for (name,) in rows) == ["Bob", "Carol"]
