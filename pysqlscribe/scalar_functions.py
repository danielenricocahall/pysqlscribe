from pysqlscribe.column import Column

ABS = "ABS"
FLOOR = "FLOOR"
CEIL = "CEIl"
SQRT = "SQRT"
ROUND = "ROUND"


def _scalar_function(scalar_function: str, column: Column | str | int) -> Column | str:
    if not isinstance(column, Column):
        return f"{scalar_function}({column})"
    return Column(f"{scalar_function}({column.name})", column.table_name)


def abs_(column: Column | str):
    return _scalar_function(ABS, column)


def floor(column: Column | str):
    return _scalar_function(FLOOR, column)


def ceil(column: Column | str):
    return _scalar_function(CEIL, column)


def sqrt(column: Column | str):
    return _scalar_function(SQRT, column)


def round_(column: Column | str, decimals: int | None = None):
    if not decimals:
        return _scalar_function(ROUND, column)
    if not isinstance(column, Column):
        return f"{ROUND}({column}, {decimals})"
    return Column(f"{ROUND}({column.name}, {decimals})", column.table_name)
