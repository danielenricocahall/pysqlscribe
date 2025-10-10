from pysqlscribe.column import Column, ExpressionColumn
from pysqlscribe.functions import ScalarFunctions


def _scalar_function(scalar_function: str, column: Column | str | int) -> Column | str:
    if not isinstance(column, Column):
        return f"{scalar_function}({column})"
    return ExpressionColumn(f"{scalar_function}({column.name})", column.table_name)


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


def ltrim(column: Column | str):
    return _scalar_function(ScalarFunctions.LTRIM, column)


def rtrim(column: Column | str):
    return _scalar_function(ScalarFunctions.RTRIM, column)


def trim(column: Column | str):
    return _scalar_function(ScalarFunctions.TRIM, column)


def reverse(column: Column | str):
    return _scalar_function(ScalarFunctions.REVERSE, column)


def round_(column: Column | str, decimals: int | None = None):
    if not decimals:
        return _scalar_function(ScalarFunctions.ROUND, column)
    if not isinstance(column, Column):
        return f"{ScalarFunctions.ROUND}({column}, {decimals})"
    return ExpressionColumn(
        f"{ScalarFunctions.ROUND}({column.name}, {decimals})", column.table_name
    )


def trunc(column: Column | str, decimals: int | None = None):
    if not decimals:
        return _scalar_function(ScalarFunctions.TRUNC, column)
    if not isinstance(column, Column):
        return f"{ScalarFunctions.TRUNC}({column}, {decimals})"
    return ExpressionColumn(
        f"{ScalarFunctions.TRUNC}({column.name}, {decimals})", column.table_name
    )


def power(base: Column | str | int, exponent: Column | str | int):
    if all(isinstance(arg, Column) for arg in (base, exponent)):
        return ExpressionColumn(
            f"{ScalarFunctions.POWER}({base.name}, {exponent.name})",
            base.table_name,
        )
    if isinstance(base, Column):
        base = base.name
    if isinstance(base, str):
        base = int(base) if base.isdigit() else base
    if isinstance(exponent, Column):
        exponent = exponent.name
    if isinstance(exponent, str):
        exponent = int(exponent) if exponent.isdigit() else exponent
    return f"{ScalarFunctions.POWER}({base}, {exponent})"


def ln(column: Column | str | int):
    return _scalar_function(ScalarFunctions.LN, column)


def exp(column: Column | str | int):
    return _scalar_function(ScalarFunctions.EXP, column)


def concat(*args: Column | str | int):
    if all(isinstance(arg, Column) for arg in args):
        return ExpressionColumn(
            f"{ScalarFunctions.CONCAT}({', '.join(arg.name for arg in args)})",
            args[0].table_name,
        )
    args = [f"'{arg}'" if not isinstance(arg, Column) else str(arg) for arg in args]
    return f"{ScalarFunctions.CONCAT}({', '.join(args)})"
