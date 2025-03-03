import pytest

from pysqlscribe.aggregate_functions import max_, min_, avg, count, AggregateFunctions
from pysqlscribe.scalar_functions import (
    abs_,
    floor,
    sqrt,
    ceil,
    round_,
    upper,
    lower,
    ScalarFunctions,
    concat,
)
from pysqlscribe.table import PostgresTable


@pytest.mark.parametrize(
    "agg_function,agg_str",
    [
        (max_, AggregateFunctions.MAX),
        (min_, AggregateFunctions.MIN),
        (avg, AggregateFunctions.AVG),
    ],
)
def test_aggregate_functions(agg_function, agg_str):
    table = PostgresTable(
        "employee", "first_name", "last_name", "store_location", "salary"
    )
    query = (
        table.select(table.store_location, agg_function(table.salary))
        .group_by(table.store_location)
        .build()
    )
    assert (
        query
        == f'SELECT "store_location",{agg_str}(salary) FROM "employee" GROUP BY "store_location"'
    )


@pytest.mark.parametrize("agg_column", [1, "first_name"])
def test_agg_function_with_non_column_object(agg_column):
    table = PostgresTable(
        "employee", "first_name", "last_name", "store_location", "salary"
    )
    query = (
        table.select(table.store_location, count(agg_column))
        .group_by(table.store_location)
        .build()
    )
    assert (
        query
        == f'SELECT "store_location",COUNT({agg_column}) FROM "employee" GROUP BY "store_location"'
    )


@pytest.mark.parametrize(
    "scalar_function,str_function",
    [
        (abs_, ScalarFunctions.ABS),
        (floor, ScalarFunctions.FLOOR),
        (sqrt, ScalarFunctions.SQRT),
        (ceil, ScalarFunctions.CEIL),
        (round_, ScalarFunctions.ROUND),
        (upper, ScalarFunctions.UPPER),
        (lower, ScalarFunctions.LOWER),
    ],
)
def test_scalar_functions(scalar_function, str_function):
    payroll_table = PostgresTable("payroll", "id", "salary", "category")
    query = payroll_table.select(scalar_function(payroll_table.salary)).build()
    assert query == f'SELECT {str_function}(salary) FROM "payroll"'


def test_concat():
    payroll_table = PostgresTable("payroll", "id", "salary", "category")
    query = payroll_table.select(concat(payroll_table.salary, "USD")).build()
    assert query == "SELECT CONCAT(salary, 'USD') FROM \"payroll\""

    query = payroll_table.select(concat(payroll_table.salary, 100)).build()
    assert query == "SELECT CONCAT(salary, '100') FROM \"payroll\""

    query = payroll_table.select(concat("USD", 100)).build()
    assert query == "SELECT CONCAT('USD', '100') FROM \"payroll\""


def test_concat_with_columns():
    payroll_table = PostgresTable(
        "payroll", "first_name", "last_name", "salary", "category"
    )
    query = payroll_table.select(
        concat(payroll_table.first_name, payroll_table.last_name).as_("full_name")
    ).build()
    assert query == 'SELECT CONCAT(first_name, last_name) AS full_name FROM "payroll"'
