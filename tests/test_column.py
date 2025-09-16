from pysqlscribe.column import (
    Column,
    Expression,
    InvalidColumnNameException,
    ExpressionColumn,
)
import pytest


def test_valid_column_initialization():
    col = Column("valid_column", "test_table")
    assert col.name == "valid_column"
    assert col.table_name == "test_table"
    assert col.fully_qualified_name == "test_table.valid_column"


def test_invalid_column_name():
    with pytest.raises(InvalidColumnNameException):
        Column("invalid column!", "test_table")


def test_column_equality():
    col1 = Column("column1", "table1")
    col2 = Column("column2", "table2")
    expr = col1 == col2
    assert isinstance(expr, Expression)
    assert str(expr) == "table1.column1 = table2.column2"


def test_column_comparisons():
    col = Column("column1", "table1")
    assert str(col == "value") == "table1.column1 = 'value'"
    assert str(col < "value") == "table1.column1 < 'value'"
    assert str(col > 100) == "table1.column1 > 100"
    assert str(col <= 50) == "table1.column1 <= 50"
    assert str(col >= col) == "table1.column1 >= table1.column1"
    assert str(col != "test") == "table1.column1 <> 'test'"


def test_column_arithmetic():
    col1 = Column("column1", "table1")
    col2 = Column("column2", "table2")

    result_add = col1 + col2
    assert isinstance(result_add, ExpressionColumn)
    assert str(result_add) == "table1.column1 + table2.column2"

    result_sub = col1 - 10
    assert isinstance(result_sub, ExpressionColumn)
    assert str(result_sub) == "table1.column1 - 10"

    result_mul = col1 * col2
    assert isinstance(result_mul, ExpressionColumn)
    assert str(result_mul) == "table1.column1 * table2.column2"

    result_div = col1 / 2 * col2
    assert isinstance(result_div, ExpressionColumn)
    assert str(result_div) == "table1.column1 / 2 * table2.column2"


def test_column_membership():
    col = Column("column1", "table1")
    assert str(col.in_(["a", "b", "c"])) == "table1.column1 IN ('a', 'b', 'c')"
    assert str(col.in_([1, 2, 3])) == "table1.column1 IN (1, 2, 3)"
    assert str(col.not_in(["x", "y"])) == "table1.column1 NOT IN ('x', 'y')"


def test_column_membership_failure_mixed_types():
    col = Column("column1", "table1")
    with pytest.raises(NotImplementedError):
        col.in_([1, "a", 3.5])


def test_column_membership_failure_unsupported_type():
    col = Column("column1", "table1")
    with pytest.raises(NotImplementedError):
        col.in_([{"key": "value"}])


def test_expression_str():
    expr = Expression("table1.column1", "=", "table2.column2")
    assert str(expr) == "table1.column1 = table2.column2"


def test_expression_repr():
    expr = Expression("table1.column1", "=", "table2.column2")
    assert repr(expr) == "Expression('table1.column1', '=', 'table2.column2')"


def test_combined_column_fqn():
    col1 = Column("column1", "table1")
    col2 = Column("column2", "table2")
    combined = col1 + col2
    assert combined.fully_qualified_name == "table1.column1 + table2.column2"


def test_column_not_implemented():
    col = Column("column1", "table1")
    with pytest.raises(NotImplementedError):
        col == ["unsupported_type"]
