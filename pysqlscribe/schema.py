import os
from typing import List

from pysqlscribe.regex_patterns import VALID_IDENTIFIER_REGEX
from pysqlscribe.table import Table


class InvalidSchemaNameException(Exception): ...


class Schema:
    def __init__(
        self,
        name: str,
        tables: List[Table | str] | None = None,
        dialect: str | None = None,
    ):
        self.name = name
        self.dialect = dialect
        self.tables = tables

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, schema_name: str):
        if not VALID_IDENTIFIER_REGEX.match(schema_name):
            raise InvalidSchemaNameException(f"Invalid schema name {schema_name}")
        self._name = schema_name

    @property
    def tables(self):
        return self._tables

    @tables.setter
    def tables(self, tables_: list[str | Table]):
        if all(isinstance(table, str) for table in tables_):
            tables_ = [Table.create(self.dialect)(table_name) for table_name in tables_]
        for table in tables_:
            setattr(self, table.table_name, table)
            table.schema = self.name
        self._tables = tables_

    @property
    def dialect(self):
        return self._dialect

    @dialect.setter
    def dialect(self, dialect_: str | None):
        if dialect_:
            self._dialect = dialect_
        elif env_set_dialect := os.environ.get("PYSQLSCRIBE_BUILDER_DIALECT"):
            self._dialect = env_set_dialect
        else:
            # the user may have provided no `dialect` - this is fine if they're directly supplying `Table` objects.
            self._dialect = dialect_
