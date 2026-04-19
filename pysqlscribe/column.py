from typing import Self, Iterable, Protocol, runtime_checkable

from pysqlscribe.alias import AliasMixin
from pysqlscribe.exceptions import InvalidColumnNameException
from pysqlscribe.functions import ScalarFunctions
from pysqlscribe.regex_patterns import (
    VALID_IDENTIFIER_REGEX,
    AGGREGATE_IDENTIFIER_REGEX,
    SCALAR_IDENTIFIER_REGEX,
    EXPRESSION_IDENTIFIER_REGEX,
)


class Expression:
    def __init__(self, left: str, operator: str, right: str):
        self.left = left
        self.operator = operator
        self.right = right

    def __str__(self):
        return f"{self.left} {self.operator} {self.right}"

    def __repr__(self):
        return f"Expression({self.left!r}, {self.operator!r}, {self.right!r})"

    def __and__(self, other: "Expression") -> "CompoundExpression":
        return CompoundExpression(self, "AND", other)

    def __or__(self, other: "Expression") -> "CompoundExpression":
        return CompoundExpression(self, "OR", other)

    def __invert__(self) -> "NotExpression":
        return NotExpression(self)


class CompoundExpression(Expression):
    def __init__(self, left: Expression, operator: str, right: Expression):
        self.left = left
        self.operator = operator
        self.right = right

    def __str__(self):
        return f"({self.left}) {self.operator} ({self.right})"

    def __repr__(self):
        return f"CompoundExpression({self.left!r}, {self.operator!r}, {self.right!r})"


class NotExpression(Expression):
    def __init__(self, inner: Expression):
        self.inner = inner
        self.left = "NOT"
        self.operator = ""
        self.right = inner

    def __str__(self):
        return f"NOT ({self.inner})"

    def __repr__(self):
        return f"NotExpression({self.inner!r})"


class OrderedColumn:
    """A column paired with a sort direction, produced by Column.asc() or Column.desc()."""

    def __init__(self, name: str, direction: str):
        self.name = name
        self.direction = direction.upper()


@runtime_checkable
class Subqueryish(Protocol):
    @property
    def select(self) -> Self: ...


class Column(AliasMixin):
    def __init__(self, name: str, table_name: str):
        self.name = name
        self.table_name = table_name

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, column_name: str):
        if not (
            VALID_IDENTIFIER_REGEX.match(column_name)
            or AGGREGATE_IDENTIFIER_REGEX.match(column_name)
            or SCALAR_IDENTIFIER_REGEX.match(column_name)
            or EXPRESSION_IDENTIFIER_REGEX.match(column_name)
        ):
            raise InvalidColumnNameException(f"Invalid column name {column_name}")
        self._name = column_name

    @property
    def fully_qualified_name(self):
        return f"{self.table_name}.{self.name}"

    def _comparison_expression(self, operator: str, other: Self | str | int):
        if isinstance(other, Column):
            return Expression(
                self.fully_qualified_name, operator, other.fully_qualified_name
            )
        elif isinstance(other, str):
            return Expression(self.fully_qualified_name, operator, f"'{other}'")
        elif isinstance(other, (int, float)):
            return Expression(self.fully_qualified_name, operator, str(other))
        raise NotImplementedError(
            "Columns can only be compared to other columns or fixed string values"
        )

    def _arithmetic_expression(self, operator: str, other: Self | str | int):
        if isinstance(other, Column):
            return ExpressionColumn(
                f"{self.fully_qualified_name} {operator} {other.fully_qualified_name}",
                self.table_name,
            )
        else:
            return ExpressionColumn(
                f"{self.fully_qualified_name} {operator} {other}", self.table_name
            )

    def _membership_expression(
        self, operator: str, other: Iterable[str | int | float] | Subqueryish
    ):
        if isinstance(other, Subqueryish):
            return Expression(self.fully_qualified_name, operator, f"({other})")
        other_list = list(other)
        if not other_list:
            raise NotImplementedError(
                "membership expressions must be created with a non-empty iterable or a subquery"
            )
        if all(isinstance(item, str) for item in other_list):
            right_side = ", ".join(f"'{item}'" for item in other_list)
            return Expression(self.fully_qualified_name, operator, f"({right_side})")
        elif all(isinstance(item, (int, float)) for item in other_list):
            right_side = ", ".join(str(item) for item in other_list)
            return Expression(self.fully_qualified_name, operator, f"({right_side})")
        else:
            raise NotImplementedError(
                "membership expressions must be created with an iterable containing items of the same type (all strings or all numbers); mixed types are not allowed"
            )

    def __str__(self):
        return self.name

    def __eq__(self, other: Self | str):
        return self._comparison_expression("=", other)

    def __lt__(self, other):
        return self._comparison_expression("<", other)

    def __gt__(self, other):
        return self._comparison_expression(">", other)

    def __le__(self, other):
        return self._comparison_expression("<=", other)

    def __ge__(self, other):
        return self._comparison_expression(">=", other)

    def __ne__(self, other):
        return self._comparison_expression("<>", other)

    def __add__(self, other):
        return self._arithmetic_expression("+", other)

    def __sub__(self, other):
        return self._arithmetic_expression("-", other)

    def __mul__(self, other):
        return self._arithmetic_expression("*", other)

    def __truediv__(self, other):
        return self._arithmetic_expression("/", other)

    def __round__(self, ndigits: int | None = None):
        round_expr = (
            f"{ScalarFunctions.ROUND}({self.name}, {ndigits})"
            if ndigits is not None
            else f"{ScalarFunctions.ROUND}({self.name})"
        )
        return ExpressionColumn(round_expr, self.table_name)

    def __abs__(self):
        return ExpressionColumn(f"{ScalarFunctions.ABS}({self.name})", self.table_name)

    def __floor__(self):
        return ExpressionColumn(
            f"{ScalarFunctions.FLOOR}({self.name})", self.table_name
        )

    def __ceil__(self):
        return ExpressionColumn(f"{ScalarFunctions.CEIL}({self.name})", self.table_name)

    def in_(self, values: Iterable[str | int | float] | Subqueryish) -> Expression:
        return self._membership_expression("IN", values)

    def not_in(self, values: Iterable[str | int | float] | Subqueryish) -> Expression:
        return self._membership_expression("NOT IN", values)

    def like(self, pattern: str) -> Expression:
        return self._comparison_expression("LIKE", pattern)

    def not_like(self, pattern: str) -> Expression:
        return self._comparison_expression("NOT LIKE", pattern)

    def ilike(self, pattern: str) -> Expression:
        return self._comparison_expression("ILIKE", pattern)

    def _between(self, low, high, operator) -> Expression:
        def resolve_column_names(low):
            if isinstance(low, Column):
                return low.fully_qualified_name
            elif isinstance(low, str):
                return f"'{low}'"
            elif isinstance(low, (int, float)):
                return str(low)

        low_expr = resolve_column_names(low)
        high_expr = resolve_column_names(high)
        return Expression(
            self.fully_qualified_name, operator, f"{low_expr} AND {high_expr}"
        )

    def between(self, low, high) -> Expression:
        return self._between(low, high, "BETWEEN")

    def not_between(self, low, high) -> Expression:
        return self._between(low, high, "NOT BETWEEN")

    def is_null(self) -> Expression:
        return Expression(self.fully_qualified_name, "IS", "NULL")

    def is_not_null(self) -> Expression:
        return Expression(self.fully_qualified_name, "IS NOT", "NULL")

    def _sort(self, direction: str) -> OrderedColumn:
        return OrderedColumn(self.name, direction)

    def asc(self) -> "OrderedColumn":
        return self._sort("ASC")

    def desc(self) -> "OrderedColumn":
        return self._sort("DESC")


class ExpressionColumn(Column):
    """Representation of a column that is the result of an arithmetic operation. Main benefit is to ensure the
    fully qualified name doesn't prepend the table name each time."""

    @property
    def fully_qualified_name(self):
        return self.name
