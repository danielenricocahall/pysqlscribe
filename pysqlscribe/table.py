from __future__ import annotations

from typing import List, Self

from pysqlscribe.alias import AliasMixin
from pysqlscribe.column import Column
from pysqlscribe.exceptions import InvalidTableNameException
from pysqlscribe.ast.joins import JoinType
from pysqlscribe.query import Query
from pysqlscribe.regex_patterns import (
    VALID_IDENTIFIER_REGEX,
)


class Table(Query, AliasMixin):
    def __init__(self, name: str, *columns, dialect: str, schema: str | None = None):
        Query.__init__(self, dialect)
        self.table_name = name
        self.schema = schema
        self.columns = columns

    def select(self, *columns) -> Self:
        columns = [
            f"{column.name}{column.alias}" if isinstance(column, Column) else column
            for column in columns
        ]
        table_name = f"{self.table_name}{self.alias}"
        return super().select(*columns).from_(table_name)

    def order_by(self, *columns) -> Self:
        columns = [
            column.name if isinstance(column, Column) else column for column in columns
        ]
        return super().order_by(*columns)

    def group_by(self, *columns) -> Self:
        columns = [
            column.name if isinstance(column, Column) else column for column in columns
        ]
        return super().group_by(*columns)

    def join(
        self,
        table: str | "Table",
        join_type: str = JoinType.INNER,
        condition: str | None = None,
    ) -> Self:
        if isinstance(table, Table):
            table = f"{table.table_name}{table.alias}"
        return super().join(table, join_type, condition)

    def insert(self, *columns, **kwargs) -> Self:
        columns = [
            column.name if isinstance(column, Column) else column for column in columns
        ]
        return super().insert(
            *columns, into=self.table_name, values=kwargs.get("values")
        )

    @property
    def table_name(self):
        if self.schema:
            return f"{self.schema}.{self._table_name}"
        return self._table_name

    @table_name.setter
    def table_name(self, table_name_: str):
        if not VALID_IDENTIFIER_REGEX.match(table_name_):
            raise InvalidTableNameException(f"Invalid table name {table_name_}")
        self._table_name = table_name_
        if getattr(self, "_columns", None):
            # ensure that we re-assign our column attributes with the correct fully qualified name
            self.columns = self.columns

    @property
    def columns(self):
        return self._columns

    @columns.setter
    def columns(self, columns_: List[str]):
        if getattr(self, "_columns", None):
            for column in self.columns:
                delattr(self, column)
        self._columns = columns_
        for column_name in columns_:
            setattr(
                self, column_name, Column(column_name, self._alias or self.table_name)
            )

    def as_(self, alias: str) -> Self:
        super().as_(alias)
        # ensure that we re-assign our column attributes with the correct fully qualified name (including
        # the alias instead of the full table name)
        self.columns = self.columns
        return self

    def __repr__(self):
        return (
            f"{self.__class__.__name__}(name={self.table_name}, columns={self.columns})"
        )
