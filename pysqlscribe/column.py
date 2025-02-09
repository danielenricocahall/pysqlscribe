from typing import Self

from pysqlscribe.regex_patterns import (
    VALID_IDENTIFIER_REGEX,
    AGGREGATE_IDENTIFIER_REGEX,
)


class InvalidColumnNameException(Exception): ...


class Expression:
    def __init__(self, left: str, operator: str, right: str):
        self.left = left
        self.operator = operator
        self.right = right

    def __str__(self):
        return f"{self.left} {self.operator} {self.right}"

    def __repr__(self):
        return f"Expression({self.left!r}, {self.operator!r}, {self.right!r})"


class Column:
    def __init__(self, name: str):
        self.name = name

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, column_name: str):
        if not (
            VALID_IDENTIFIER_REGEX.match(column_name)
            or AGGREGATE_IDENTIFIER_REGEX.match(column_name)
        ):
            raise InvalidColumnNameException(f"Invalid column name {column_name}")
        self._name = column_name

    def _expression(self, operator: str, other: Self | str | int):
        if isinstance(other, Column):
            return Expression(self.name, operator, other.name)
        elif isinstance(other, str):
            return Expression(self.name, operator, f"'{other}'")
        elif isinstance(other, int):
            return Expression(self.name, operator, str(other))
        raise NotImplementedError(
            "Columns can only be compared to other columns or fixed string values"
        )

    def __str__(self):
        return self.name

    def __eq__(self, other: Self | str):
        return self._expression("=", other)

    def __lt__(self, other):
        return self._expression("<", other)

    def __gt__(self, other):
        return self._expression(">", other)

    def __le__(self, other):
        return self._expression("<=", other)

    def __ge__(self, other):
        return self._expression(">=", other)
