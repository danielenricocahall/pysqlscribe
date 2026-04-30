import pytest

from pysqlscribe.column import case_
from pysqlscribe.cte import with_
from pysqlscribe.query import Query
from pysqlscribe.table import Table


@pytest.mark.parametrize(
    "dialect,placeholder",
    [
        ("postgres", "%s"),
        ("mysql", "%s"),
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


def test_postgres_param_order_matches_sql_order():
    table = Table("employees", "salary", "bonus", dialect="postgres")
    sql, params = (
        table.select("salary")
        .where(table.salary > 1000)
        .where(table.bonus < 500)
        .build(parameterize=True)
    )
    assert sql.count("%s") == 2
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
    assert f"employees.salary {operator} %s" in sql
    assert params == [10]


@pytest.mark.parametrize("dialect", ["postgres", "mysql", "sqlite", "oracle"])
def test_in_iterable_binds_each_value(dialect):
    table = Table("employees", "department_id", dialect=dialect)
    sql, params = (
        table.select("department_id")
        .where(table.department_id.in_([1, 2, 3]))
        .build(parameterize=True)
    )
    assert params == [1, 2, 3]


def test_postgres_in_renders_each_position():
    table = Table("employees", "department_id", dialect="postgres")
    sql, params = (
        table.select("department_id")
        .where(table.department_id.in_([1, 2, 3]))
        .build(parameterize=True)
    )
    assert sql.endswith("IN (%s, %s, %s)")
    assert params == [1, 2, 3]


def test_not_in_iterable_binds_each_value():
    table = Table("employees", "department_id", dialect="mysql")
    sql, params = (
        table.select("department_id")
        .where(table.department_id.not_in(["a", "b"]))
        .build(parameterize=True)
    )
    assert sql.endswith("NOT IN (%s, %s)")
    assert params == ["a", "b"]


def test_between_binds_low_and_high():
    table = Table("employees", "salary", dialect="postgres")
    sql, params = (
        table.select("salary")
        .where(table.salary.between(1000, 2000))
        .build(parameterize=True)
    )
    assert "BETWEEN %s AND %s" in sql
    assert params == [1000, 2000]


def test_not_between_binds_low_and_high():
    table = Table("employees", "salary", dialect="mysql")
    sql, params = (
        table.select("salary")
        .where(table.salary.not_between(1000, 2000))
        .build(parameterize=True)
    )
    assert "NOT BETWEEN %s AND %s" in sql
    assert params == [1000, 2000]


def test_like_pattern_is_bound():
    table = Table("users", "name", dialect="postgres")
    sql, params = (
        table.select("name").where(table.name.like("A%")).build(parameterize=True)
    )
    assert "LIKE %s" in sql
    assert params == ["A%"]


def test_not_like_pattern_is_bound():
    table = Table("users", "name", dialect="postgres")
    sql, params = (
        table.select("name").where(table.name.not_like("A%")).build(parameterize=True)
    )
    assert "NOT LIKE %s" in sql
    assert params == ["A%"]


def test_ilike_pattern_is_bound():
    table = Table("users", "name", dialect="postgres")
    sql, params = (
        table.select("name").where(table.name.ilike("a%")).build(parameterize=True)
    )
    assert "ILIKE %s" in sql
    assert params == ["a%"]


def test_compound_and_preserves_param_order():
    table = Table("employees", "salary", "bonus", dialect="postgres")
    sql, params = (
        table.select("salary")
        .where((table.salary > 1000) & (table.bonus < 500))
        .build(parameterize=True)
    )
    assert "AND" in sql
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
    assert "%s" in sql
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
    assert "%s" in sql
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


def test_case_then_else_values_bound():
    table = Table("employees", "salary", "dept", dialect="postgres")
    band = (
        case_()
        .when(table.salary > 100000, 1)
        .when(table.salary > 50000, 2)
        .else_(3)
        .as_("salary_band")
    )
    sql, params = table.select(table.dept, band).build(parameterize=True)
    # 2 condition literals + 2 THEN literals + 1 ELSE literal = 5 placeholders
    assert sql.count("%s") == 5
    assert "CASE WHEN" in sql
    assert params == [100000, 1, 50000, 2, 3]


def test_case_in_where_threads_collector():
    table = Table("employees", "salary", "dept", dialect="postgres")
    sql, params = (
        table.select("dept")
        .where(table.salary > 1000)
        .order_by(case_().when(table.dept == "Sales", 1).else_(2))
        .build(parameterize=True)
    )
    assert params == [1000, "Sales", 1, 2]


def test_subquery_in_in_clause_propagates_params():
    employees = Table("employees", "department_id", dialect="postgres")
    departments = Table("departments", "id", "name", dialect="postgres")
    subquery = departments.select("id").where(departments.name == "Engineering")
    sql, params = (
        employees.select()
        .where(employees.department_id.in_(subquery))
        .build(parameterize=True)
    )
    # Engineering is the only literal — and it must be a placeholder, not inlined
    assert "Engineering" not in sql
    assert "%s" in sql
    assert params == ["Engineering"]


def test_subquery_in_from_propagates_params():
    inner = Query("postgres")
    inner.select("name").from_("employees").where("salary > 1000")
    inner_with_param = Query("postgres")
    employees = Table("employees", "name", "salary", dialect="postgres")
    inner_with_param.select("name").from_(employees).where(employees.salary > 1000)
    outer = Query("postgres")
    sql, params = (
        outer.select("*").from_(inner_with_param.as_("e")).build(parameterize=True)
    )
    assert "1000" not in sql
    assert "%s" in sql
    assert params == [1000]


def test_outer_where_after_from_subquery_maintains_param_order():
    employees = Table("employees", "name", "salary", dialect="postgres")
    inner = Query("postgres")
    inner.select("name", "salary").from_(employees).where(employees.salary > 1000)
    outer = Query("postgres")
    outer_table = Table("e", "name", "salary", dialect="postgres")
    sql, params = (
        outer.select("name")
        .from_(inner.as_("e"))
        .where(outer_table.salary < 5000)
        .build(parameterize=True)
    )
    assert sql.count("%s") == 2
    assert params == [1000, 5000]


def test_cte_subquery_params_propagate():
    employees = Table("employees", "name", "salary", dialect="postgres")
    high_earners = employees.select("name", "salary").where(employees.salary > 100000)
    sql, params = (
        with_("HighEarners", dialect="postgres")
        .as_(high_earners)
        .select("*")
        .from_("HighEarners")
        .build(parameterize=True)
    )
    assert "100000" not in sql
    assert "%s" in sql
    assert params == [100000]


def test_cte_outer_where_after_cte_maintains_param_order():
    employees = Table("employees", "name", "salary", dialect="postgres")
    cte_query = employees.select("name", "salary").where(employees.salary > 100000)
    high_earners = Table("HighEarners", "name", "salary", dialect="postgres")
    sql, params = (
        with_("HighEarners", dialect="postgres")
        .as_(cte_query)
        .select("name")
        .from_(high_earners)
        .where(high_earners.salary < 500000)
        .build(parameterize=True)
    )
    assert sql.count("%s") == 2
    assert params == [100000, 500000]


def test_join_on_literal_is_bound():
    employees = Table("employees", "id", "role", dialect="postgres")
    payroll = Table("payroll", "id", "employee_id", dialect="postgres")
    sql, params = (
        employees.select("id")
        .join(payroll, condition=(employees.role == "manager"))
        .build(parameterize=True)
    )
    assert "manager" not in sql
    assert "%s" in sql
    assert params == ["manager"]


def test_union_propagates_params_from_both_sides():
    a = Table("employees", "name", "salary", dialect="postgres")
    b = Table("contractors", "name", "salary", dialect="postgres")
    left = a.select("name").where(a.salary > 1000)
    right = b.select("name").where(b.salary > 2000)
    sql, params = left.union(right).build(parameterize=True)
    assert sql.count("%s") == 2
    assert params == [1000, 2000]
