from pysqlscribe.column import (
    Case,
    Column,
    CompoundExpression,
    Expression,
    InvalidColumnsError,
    ExpressionColumn,
    NotExpression,
    case_,
)
import pytest


def test_valid_column_initialization():
    col = Column("valid_column", "test_table")
    assert col.name == "valid_column"
    assert col.table_name == "test_table"
    assert col.fully_qualified_name == "test_table.valid_column"


def test_invalid_column_name():
    with pytest.raises(InvalidColumnsError):
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


def test_column_like_comparisons():
    col = Column("column1", "table1")
    assert str(col.like("%pattern%")) == "table1.column1 LIKE '%pattern%'"
    assert str(col.not_like("exact")) == "table1.column1 NOT LIKE 'exact'"
    assert (
        str(col.ilike("case_insensitive")) == "table1.column1 ILIKE 'case_insensitive'"
    )


def test_column_between():
    col = Column("column1", "table1")
    between_expr = col.between(10, 20)
    assert str(between_expr) == "table1.column1 BETWEEN 10 AND 20"


def test_column_not_between():
    col = Column("column1", "table1")
    not_between_expr = col.not_between(30, 40)
    assert str(not_between_expr) == "table1.column1 NOT BETWEEN 30 AND 40"


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


def test_column_is_null():
    col = Column("column1", "table1")
    expr = col.is_null()
    assert isinstance(expr, Expression)
    assert str(expr) == "table1.column1 IS NULL"


def test_column_is_not_null():
    col = Column("column1", "table1")
    expr = col.is_not_null()
    assert isinstance(expr, Expression)
    assert str(expr) == "table1.column1 IS NOT NULL"


def test_expression_and():
    col = Column("column1", "table1")
    expr = (col == 1) & (col > 5)
    assert isinstance(expr, CompoundExpression)
    assert str(expr) == "(table1.column1 = 1) AND (table1.column1 > 5)"


def test_expression_or():
    col = Column("column1", "table1")
    expr = (col == 1) | (col == 2)
    assert isinstance(expr, CompoundExpression)
    assert str(expr) == "(table1.column1 = 1) OR (table1.column1 = 2)"


def test_expression_not():
    col = Column("column1", "table1")
    expr = ~col.is_null()
    assert isinstance(expr, NotExpression)
    assert str(expr) == "NOT (table1.column1 IS NULL)"


def test_expression_mixed_precedence():
    col = Column("column1", "table1")
    other = Column("column2", "table1")
    expr = (col == 1) & ((other > 5) | other.is_null())
    assert (
        str(expr)
        == "(table1.column1 = 1) AND ((table1.column2 > 5) OR (table1.column2 IS NULL))"
    )


def test_expression_not_compound():
    col = Column("column1", "table1")
    expr = ~((col == 1) | (col == 2))
    assert str(expr) == "NOT ((table1.column1 = 1) OR (table1.column1 = 2))"


def test_case_basic_with_else():
    col = Column("dept", "employees")
    expr = case_().when(col == "Sales", "sales").else_("other")
    assert isinstance(expr, Case)
    assert (
        str(expr) == "CASE WHEN employees.dept = 'Sales' THEN 'sales' ELSE 'other' END"
    )


def test_case_without_else():
    col = Column("dept", "employees")
    expr = case_().when(col == "Sales", "sales")
    assert str(expr) == "CASE WHEN employees.dept = 'Sales' THEN 'sales' END"


def test_case_multiple_whens_numeric():
    col = Column("salary", "employees")
    expr = case_().when(col > 100000, 1).when(col > 50000, 2).else_(3)
    assert (
        str(expr)
        == "CASE WHEN employees.salary > 100000 THEN 1 WHEN employees.salary > 50000 THEN 2 ELSE 3 END"
    )


def test_case_column_valued_then():
    dept = Column("dept", "employees")
    salary = Column("salary", "employees")
    expr = case_().when(dept == "Sales", salary).else_(0)
    assert (
        str(expr)
        == "CASE WHEN employees.dept = 'Sales' THEN employees.salary ELSE 0 END"
    )


def test_case_with_alias():
    col = Column("dept", "employees")
    expr = case_().when(col == "Sales", 1).else_(0).as_("is_sales")
    assert (
        str(expr) == "CASE WHEN employees.dept = 'Sales' THEN 1 ELSE 0 END AS is_sales"
    )


def test_case_empty_raises():
    with pytest.raises(ValueError):
        str(case_())


def test_case_unsupported_value_type():
    col = Column("dept", "employees")
    with pytest.raises(NotImplementedError):
        str(case_().when(col == "Sales", ["list"]))


def test_column_not_implemented():
    col = Column("column1", "table1")
    with pytest.raises(NotImplementedError):
        col == ["unsupported_type"]
