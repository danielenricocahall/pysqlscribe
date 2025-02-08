from typing import Self

from pysqlscribe.regex_patterns import VALID_IDENTIFIER_REGEX


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
        if not VALID_IDENTIFIER_REGEX.match(column_name):
            raise InvalidColumnNameException(f"Invalid table name {column_name}")
        self._name = column_name

    def __eq__(self, other: Self | str):
        if isinstance(other, Column):
            return Expression(self.name, "=", other.name)
        elif isinstance(other, str):
            return Expression(self.name, "=", f"'{other}'")
        return NotImplementedError(
            "Columns can only be compared to other columns or fixed string values"
        )
