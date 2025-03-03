from pysqlscribe.column import Column, ExpressionColumn
from pysqlscribe.functions import AggregateFunctions


def _aggregate_function(agg_function: str, column: Column | str | int):
    if not isinstance(column, Column):
        return f"{agg_function}({column})"
    return ExpressionColumn(f"{agg_function}({column.name})", column.table_name)


def max_(column: Column | str) -> Column | str:
    return _aggregate_function(AggregateFunctions.MAX, column)


def sum_(column: Column | str) -> Column:
    return _aggregate_function(AggregateFunctions.SUM, column)


def min_(column: Column | str) -> Column | str:
    return _aggregate_function(AggregateFunctions.MIN, column)


def avg(column: Column | str) -> Column | str:
    return _aggregate_function(AggregateFunctions.AVG, column)


def count(column: Column | str | int) -> Column | str:
    return _aggregate_function(AggregateFunctions.COUNT, column)


def distinct(column: Column | str) -> Column | str:
    return _aggregate_function(AggregateFunctions.DISTINCT, column)
