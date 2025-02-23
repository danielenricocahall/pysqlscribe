from pysqlscribe.column import Column

MAX = "MAX"
MIN = "MIN"
AVG = "AVG"
COUNT = "COUNT"
SUM = "SUM"
DISTINCT = "DISTINCT"


def _aggregate_function(agg_function: str, column: Column | str | int):
    if not isinstance(column, Column):
        return f"{agg_function}({column})"
    return Column(f"{agg_function}({column.name})", column.table_name)


def max_(column: Column | str) -> Column | str:
    return _aggregate_function(MAX, column)


def sum_(column: Column | str) -> Column:
    return _aggregate_function(SUM, column)


def min_(column: Column | str) -> Column | str:
    return _aggregate_function(MIN, column)


def avg(column: Column | str) -> Column | str:
    return _aggregate_function(AVG, column)


def count(column: Column | str | int) -> Column | str:
    return _aggregate_function(COUNT, column)


def distinct(column: Column | str) -> Column | str:
    return _aggregate_function(DISTINCT, column)
