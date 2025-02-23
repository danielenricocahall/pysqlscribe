from pysqlscribe.column import Column
from pysqlscribe.functions import ScalarFunctions


def _scalar_function(scalar_function: str, column: Column | str | int) -> Column | str:
    if not isinstance(column, Column):
        return f"{scalar_function}({column})"
    return Column(f"{scalar_function}({column.name})", column.table_name)


def abs_(column: Column | str):
    return _scalar_function(ScalarFunctions.ABS, column)


def floor(column: Column | str):
    return _scalar_function(ScalarFunctions.FLOOR, column)


def ceil(column: Column | str):
    return _scalar_function(ScalarFunctions.CEIL, column)


def sqrt(column: Column | str):
    return _scalar_function(ScalarFunctions.SQRT, column)


def sign(column: Column | str):
    return _scalar_function(ScalarFunctions.SIGN, column)


def length(column: Column | str):
    return _scalar_function(ScalarFunctions.LENGTH, column)


def upper(column: Column | str):
    return _scalar_function(ScalarFunctions.UPPER, column)


def lower(column: Column | str):
    return _scalar_function(ScalarFunctions.LOWER, column)


def round_(column: Column | str, decimals: int | None = None):
    if not decimals:
        return _scalar_function(ScalarFunctions.ROUND, column)
    if not isinstance(column, Column):
        return f"{ScalarFunctions.ROUND}({column}, {decimals})"
    return Column(
        f"{ScalarFunctions.ROUND}({column.name}, {decimals})", column.table_name
    )
