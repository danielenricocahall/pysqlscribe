from typing import List

from pyquerybuilder.table import Table


class Schema:
    def __init__(self, name: str, tables: List[Table | str] | None = None):
        self.name = name
        self.tables = tables

    @property
    def tables(self):
        return self._tables

    @tables.setter
    def tables(self, tables_: list[str | Table]):
        if all(isinstance(table, str) for table in tables_):
            tables_ = [Table(table_name) for table_name in tables_]
        for table in tables_:
            setattr(self, table.name, table)
            table.schema = self.name
        self._tables = tables_
