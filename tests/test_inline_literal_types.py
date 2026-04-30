"""Coverage for the inline (non-parameterized) literal-rendering path across
the four supported dialects. Booleans, dates, datetimes, and Decimals all
need to round-trip without raising NotImplementedError.
"""

import datetime
from decimal import Decimal

import pytest

from pysqlscribe.column import Column
from pysqlscribe.dialects.base import DialectRegistry
from pysqlscribe.params import ansi_escape_value
from pysqlscribe.table import Table


ALL_DIALECTS = ["postgres", "mysql", "sqlite", "oracle"]


# ---------------------------------------------------------------------------
# bool rendering — the per-dialect choice. Postgres uses ANSI TRUE/FALSE;
# the rest emit 1/0 because their engines lack a real boolean type.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "dialect_name,expected_true,expected_false",
    [
        ("postgres", "TRUE", "FALSE"),
        ("mysql", "1", "0"),
        ("sqlite", "1", "0"),
        ("oracle", "1", "0"),
    ],
)
def test_bool_literal_rendering_per_dialect(
    dialect_name, expected_true, expected_false
):
    dialect = DialectRegistry.get_dialect(dialect_name)
    assert dialect.escape_value(True) == expected_true
    assert dialect.escape_value(False) == expected_false


@pytest.mark.parametrize(
    "dialect_name,expected_true",
    [
        ("postgres", "TRUE"),
        ("mysql", "1"),
        ("sqlite", "1"),
        ("oracle", "1"),
    ],
)
def test_bool_in_where_clause_per_dialect(dialect_name, expected_true):
    table = Table("flags", "id", "is_active", dialect=dialect_name)
    sql = table.select("id").where(table.is_active == True).build()  # noqa: E712
    assert sql.endswith(f"= {expected_true}")


def test_postgres_bool_is_not_rendered_as_int():
    """Regression: isinstance(True, int) is True in Python, so the bool branch
    in escape_value must come before the int branch — otherwise postgres would
    emit `1` / `0` instead of `TRUE` / `FALSE`.
    """
    dialect = DialectRegistry.get_dialect("postgres")
    rendered_true = dialect.escape_value(True)
    rendered_false = dialect.escape_value(False)
    assert rendered_true == "TRUE"
    assert rendered_false == "FALSE"
    # And confirm we didn't accidentally fall through to str(int(bool)).
    assert rendered_true != "1"
    assert rendered_false != "0"


def test_ansi_fallback_bool_is_not_rendered_as_int():
    """Same MRO trap, exercised against the dialect-less ANSI fallback path."""
    assert ansi_escape_value(True) == "TRUE"
    assert ansi_escape_value(False) == "FALSE"


# ---------------------------------------------------------------------------
# date / datetime — same ISO formatting across all dialects.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("dialect_name", ALL_DIALECTS)
def test_date_literal_rendering(dialect_name):
    dialect = DialectRegistry.get_dialect(dialect_name)
    assert dialect.escape_value(datetime.date(2026, 4, 28)) == "'2026-04-28'"


@pytest.mark.parametrize("dialect_name", ALL_DIALECTS)
def test_datetime_literal_rendering(dialect_name):
    dialect = DialectRegistry.get_dialect(dialect_name)
    rendered = dialect.escape_value(datetime.datetime(2026, 4, 28, 14, 30, 0))
    assert rendered == "'2026-04-28 14:30:00'"


@pytest.mark.parametrize("dialect_name", ALL_DIALECTS)
def test_datetime_with_microseconds_preserved(dialect_name):
    dialect = DialectRegistry.get_dialect(dialect_name)
    rendered = dialect.escape_value(datetime.datetime(2026, 4, 28, 14, 30, 0, 123456))
    assert rendered == "'2026-04-28 14:30:00.123456'"


@pytest.mark.parametrize("dialect_name", ALL_DIALECTS)
def test_date_in_where_clause(dialect_name):
    table = Table("events", "id", "occurred_on", dialect=dialect_name)
    sql = (
        table.select("id")
        .where(table.occurred_on == datetime.date(2026, 4, 28))
        .build()
    )
    assert sql.endswith("= '2026-04-28'")


@pytest.mark.parametrize("dialect_name", ALL_DIALECTS)
def test_datetime_in_where_clause(dialect_name):
    table = Table("events", "id", "created_at", dialect=dialect_name)
    sql = (
        table.select("id")
        .where(table.created_at == datetime.datetime(2026, 4, 28, 14, 30, 0))
        .build()
    )
    assert sql.endswith("= '2026-04-28 14:30:00'")


# ---------------------------------------------------------------------------
# Decimal — rendered like int / float, no quoting.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("dialect_name", ALL_DIALECTS)
def test_decimal_literal_rendering(dialect_name):
    dialect = DialectRegistry.get_dialect(dialect_name)
    assert dialect.escape_value(Decimal("3.14")) == "3.14"
    assert dialect.escape_value(Decimal("0")) == "0"
    assert dialect.escape_value(Decimal("-1.5")) == "-1.5"


@pytest.mark.parametrize("dialect_name", ALL_DIALECTS)
def test_decimal_in_where_clause(dialect_name):
    table = Table("ledger", "id", "amount", dialect=dialect_name)
    sql = table.select("id").where(table.amount > Decimal("3.14")).build()
    assert sql.endswith("> 3.14")


# ---------------------------------------------------------------------------
# ANSI fallback path — exercised when no dialect is attached to the column.
# ---------------------------------------------------------------------------


def test_ansi_fallback_renders_all_new_types():
    col = Column("col", "tbl")  # no dialect
    assert str(col == True) == "tbl.col = TRUE"  # noqa: E712
    assert str(col == False) == "tbl.col = FALSE"  # noqa: E712
    assert str(col == datetime.date(2026, 4, 28)) == "tbl.col = '2026-04-28'"
    assert (
        str(col == datetime.datetime(2026, 4, 28, 14, 30, 0))
        == "tbl.col = '2026-04-28 14:30:00'"
    )
    assert str(col == Decimal("3.14")) == "tbl.col = 3.14"
