from pysqlscribe.column import Column

MAX = "MAX"
MIN = "MIN"
AVG = "AVG"
COUNT = "COUNT"
SUM = "SUM"
DISTINCT = "DISTINCT"


def max_(column: Column) -> Column:
    return Column(f"{MAX}({column.name})")


def sum_(column: Column) -> Column:
    return Column(f"{SUM}({column.name})")


def min_(column: Column) -> Column:
    return Column(f"{MIN}({column.name})")


def avg(column: Column) -> Column:
    return Column(f"{AVG}({column.name})")


def count(column: Column) -> Column:
    return Column(f"{COUNT}({column.name})")


def distinct(column: Column) -> Column:
    return Column(f"{DISTINCT}({column.name})")
