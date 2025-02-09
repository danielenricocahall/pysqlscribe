from abc import ABC
from typing import List

from pysqlscribe.column import Column
from pysqlscribe.query import QueryRegistry
from pysqlscribe.regex_patterns import (
    VALID_IDENTIFIER_REGEX,
)

EVERYTHING = "*"


class InvalidColumnsException(Exception): ...


class InvalidTableNameException(Exception): ...


class Table(ABC):
    __cache: dict[str, type["Table"]] = {}

    def __init__(self, name: str, *columns, schema: str | None = None):
        self.name = name
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
                    column.name if isinstance(column, Column) else column
                    for column in columns
                ]
                return super().select(*columns).from_(self.name)

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

        # Set a meaningful class name
        DynamicTable.__name__ = class_name
        cls.__cache[class_name] = DynamicTable

        return DynamicTable

    @property
    def name(self):
        if self.schema:
            return f"{self.schema}.{self._name}"
        return self._name

    @name.setter
    def name(self, table_name: str):
        if not VALID_IDENTIFIER_REGEX.match(table_name):
            raise InvalidTableNameException(f"Invalid table name {table_name}")
        self._name = table_name

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
            setattr(self, column_name, Column(column_name))


MySQLTable = Table.create("mysql")
OracleTable = Table.create("oracle")
PostgresTable = Table.create("postgres")
SqliteTable = Table.create("sqlite")
