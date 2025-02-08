from abc import ABC
from typing import List

from pysqlscribe.query import QueryRegistry
from pysqlscribe.regex_patterns import VALID_IDENTIFIER_REGEX


class InvalidFieldsException(Exception): ...


class InvalidTableNameException(Exception): ...


class Table(ABC):
    __cache: dict[str, type["Table"]] = {}

    def __init__(self, name: str, *fields, schema: str | None = None):
        self.name = name
        self.schema = schema
        self.fields = fields

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

            def select(self, *fields):
                try:
                    assert all(hasattr(self, field) for field in fields)
                    return super().select(*fields).from_(self.name)
                except AssertionError:
                    raise InvalidFieldsException(
                        f"Table {self.name} doesn't have one or more of the fields provided"
                    )

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
    def fields(self):
        return self._fields

    @fields.setter
    def fields(self, fields_: List[str]):
        if getattr(self, "_fields", None):
            for field in self.fields:
                delattr(self, field)
        self._fields = fields_
        for field in fields_:
            setattr(self, field, None)


MySQLTable = Table.create("mysql")
OracleTable = Table.create("oracle")
PostgresTable = Table.create("postgres")
SqliteTable = Table.create("sqlite")
