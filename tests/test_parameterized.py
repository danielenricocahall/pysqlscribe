import pytest

from pysqlscribe.query import Query
from pysqlscribe.table import Table


@pytest.mark.parametrize(
    "dialect,placeholder",
    [
        ("postgres", "$1"),
        ("mysql", "?"),
        ("sqlite", "?"),
        ("oracle", ":1"),
    ],
)
def test_eq_comparison_emits_placeholder_per_dialect(dialect, placeholder):
    table = Table("employees", "salary", dialect=dialect)
    sql, params = (
        table.select("salary").where(table.salary == 1000).build(parameterize=True)
    )
    assert sql.endswith(f"WHERE employees.salary = {placeholder}")
    assert params == [1000]


@pytest.mark.parametrize("dialect", ["postgres", "mysql", "sqlite", "oracle"])
def test_string_literal_is_bound_not_inlined(dialect):
    table = Table("users", "name", dialect=dialect)
    sql, params = (
        table.select("name").where(table.name == "O'Brien").build(parameterize=True)
    )
    assert "O'Brien" not in sql
    assert "O''Brien" not in sql
    assert params == ["O'Brien"]


def test_postgres_numeric_placeholders_increment():
    table = Table("employees", "salary", "bonus", dialect="postgres")
    sql, params = (
        table.select("salary")
        .where(table.salary > 1000)
        .where(table.bonus < 500)
        .build(parameterize=True)
    )
    assert "$1" in sql and "$2" in sql
    assert sql.index("$1") < sql.index("$2")
    assert params == [1000, 500]


def test_oracle_named_placeholders_increment():
    table = Table("employees", "salary", "bonus", dialect="oracle")
    sql, params = (
        table.select("salary")
        .where(table.salary > 1000)
        .where(table.bonus < 500)
        .build(parameterize=True)
    )
    assert ":1" in sql and ":2" in sql
    assert params == [1000, 500]


@pytest.mark.parametrize(
    "comparison,operator",
    [
        (lambda c: c < 10, "<"),
        (lambda c: c > 10, ">"),
        (lambda c: c <= 10, "<="),
        (lambda c: c >= 10, ">="),
        (lambda c: c != 10, "<>"),
    ],
)
def test_all_comparison_operators_bind(comparison, operator):
    table = Table("employees", "salary", dialect="postgres")
    sql, params = (
        table.select("salary").where(comparison(table.salary)).build(parameterize=True)
    )
    assert f"employees.salary {operator} $1" in sql
    assert params == [10]


@pytest.mark.parametrize("dialect", ["postgres", "mysql", "sqlite", "oracle"])
def test_in_iterable_binds_each_value(dialect):
    table = Table("employees", "department_id", dialect=dialect)
    sql, params = (
        table.select("department_id")
        .where(table.department_id.in_([1, 2, 3]))
        .build(parameterize=True)
    )
    assert sql.count("$") + sql.count("?") + sql.count(":") == 3
    assert params == [1, 2, 3]


def test_postgres_in_renders_each_position():
    table = Table("employees", "department_id", dialect="postgres")
    sql, params = (
        table.select("department_id")
        .where(table.department_id.in_([1, 2, 3]))
        .build(parameterize=True)
    )
    assert sql.endswith("IN ($1, $2, $3)")
    assert params == [1, 2, 3]


def test_not_in_iterable_binds_each_value():
    table = Table("employees", "department_id", dialect="mysql")
    sql, params = (
        table.select("department_id")
        .where(table.department_id.not_in(["a", "b"]))
        .build(parameterize=True)
    )
    assert sql.endswith("NOT IN (?, ?)")
    assert params == ["a", "b"]


def test_between_binds_low_and_high():
    table = Table("employees", "salary", dialect="postgres")
    sql, params = (
        table.select("salary")
        .where(table.salary.between(1000, 2000))
        .build(parameterize=True)
    )
    assert "BETWEEN $1 AND $2" in sql
    assert params == [1000, 2000]


def test_not_between_binds_low_and_high():
    table = Table("employees", "salary", dialect="mysql")
    sql, params = (
        table.select("salary")
        .where(table.salary.not_between(1000, 2000))
        .build(parameterize=True)
    )
    assert "NOT BETWEEN ? AND ?" in sql
    assert params == [1000, 2000]


def test_like_pattern_is_bound():
    table = Table("users", "name", dialect="postgres")
    sql, params = (
        table.select("name").where(table.name.like("A%")).build(parameterize=True)
    )
    assert "LIKE $1" in sql
    assert params == ["A%"]


def test_not_like_pattern_is_bound():
    table = Table("users", "name", dialect="postgres")
    sql, params = (
        table.select("name").where(table.name.not_like("A%")).build(parameterize=True)
    )
    assert "NOT LIKE $1" in sql
    assert params == ["A%"]


def test_ilike_pattern_is_bound():
    table = Table("users", "name", dialect="postgres")
    sql, params = (
        table.select("name").where(table.name.ilike("a%")).build(parameterize=True)
    )
    assert "ILIKE $1" in sql
    assert params == ["a%"]


def test_compound_and_preserves_param_order():
    table = Table("employees", "salary", "bonus", dialect="postgres")
    sql, params = (
        table.select("salary")
        .where((table.salary > 1000) & (table.bonus < 500))
        .build(parameterize=True)
    )
    assert "($1)" in sql or "$1" in sql
    assert "($2)" in sql or "$2" in sql
    assert params == [1000, 500]


def test_compound_or_preserves_param_order():
    table = Table("employees", "salary", "bonus", dialect="postgres")
    sql, params = (
        table.select("salary")
        .where((table.salary > 1000) | (table.bonus < 500))
        .build(parameterize=True)
    )
    assert "OR" in sql
    assert params == [1000, 500]


def test_not_expression_binds_inner():
    table = Table("employees", "salary", dialect="postgres")
    sql, params = (
        table.select("salary").where(~(table.salary > 1000)).build(parameterize=True)
    )
    assert "NOT" in sql
    assert "$1" in sql
    assert params == [1000]


def test_having_clause_also_parameterized():
    table = Table("employees", "salary", "department_id", dialect="postgres")
    sql, params = (
        table.select("department_id")
        .group_by("department_id")
        .having(table.salary > 1000)
        .build(parameterize=True)
    )
    assert "HAVING" in sql
    assert "$1" in sql
    assert params == [1000]


def test_default_build_unchanged_returns_str():
    table = Table("employees", "salary", dialect="postgres")
    result = table.select("salary").where(table.salary == 1000).build()
    assert isinstance(result, str)
    assert "1000" in result


def test_default_build_inlines_string_literal_with_escape():
    table = Table("users", "name", dialect="postgres")
    result = table.select("name").where(table.name == "O'Brien").build()
    assert "'O''Brien'" in result


def test_is_null_uses_no_placeholder():
    table = Table("employees", "bonus", dialect="postgres")
    sql, params = (
        table.select("bonus").where(table.bonus.is_null()).build(parameterize=True)
    )
    assert "IS NULL" in sql
    assert params == []


def test_query_class_directly_returns_tuple():
    q = Query("postgres")
    sql, params = (
        q.select("name")
        .from_("users")
        .where("name = 'still-raw'")
        .build(parameterize=True)
    )
    assert isinstance(sql, str)
    assert isinstance(params, list)
    assert params == []
