from abc import ABC
from typing import List, Self

from pysqlscribe.alias import AliasMixin
from pysqlscribe.column import Column
from pysqlscribe.query import QueryRegistry, JoinType
from pysqlscribe.regex_patterns import (
    VALID_IDENTIFIER_REGEX,
)


class InvalidColumnsException(Exception): ...


class InvalidTableNameException(Exception): ...


class Table(ABC, AliasMixin):
    __cache: dict[str, type["Table"]] = {}

    def __init__(self, name: str, *columns, schema: str | None = None):
        self.table_name = name
        self.schema = schema
        self.columns = columns

    @classmethod
    def create(cls, dialect: str):
        if dialect not in QueryRegistry.builders:
            raise ValueError(f"Unsupported dialect: {dialect}")

        class_name = f"{dialect.capitalize()}Table"

        if class_name in cls.__cache:
            return cls.__cache[class_name]

        query_class = QueryRegistry.get_builder(dialect).__class__

        class DynamicTable(query_class, Table):
            def __init__(self, name: str, *fields, schema: str | None = None):
                query_class.__init__(self)
                Table.__init__(self, name, *fields, schema=schema)

            def select(self, *columns):
                columns = [
                    f"{column.name}{column.alias}"
                    if isinstance(column, Column)
                    else column
                    for column in columns
                ]
                table_name = f"{self.table_name}{self.alias}"
                return super().select(*columns).from_(table_name)

            def order_by(self, *columns):
                columns = [
                    column.name if isinstance(column, Column) else column
                    for column in columns
                ]
                return super().order_by(*columns)

            def group_by(self, *columns):
                columns = [
                    column.name if isinstance(column, Column) else column
                    for column in columns
                ]
                return super().group_by(*columns)

            def join(
                self,
                table: str | Table,
                join_type: str = JoinType.INNER,
                condition: str | None = None,
            ) -> Self:
                if isinstance(table, Table):
                    table = f"{table.table_name}{table.alias}"
                return super().join(table, join_type, condition)

            def insert(self, *columns, **kwargs) -> Self:
                """
                Override the insert method to ensure it uses the correct table name.
                """
                columns = [
                    column.name if isinstance(column, Column) else column
                    for column in columns
                ]
                return super().insert(
                    *columns, into=self.table_name, values=kwargs.get("values")
                )

            def __repr__(self):
                return f"{self.__class__.__name__}(name={self.table_name}, columns={self.columns})"

        # Set a meaningful class name
        DynamicTable.__name__ = class_name
        cls.__cache[class_name] = DynamicTable

        return DynamicTable

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


MySQLTable = Table.create("mysql")
OracleTable = Table.create("oracle")
PostgresTable = Table.create("postgres")
SqliteTable = Table.create("sqlite")
