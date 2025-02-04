from typing import List

from pyquerybuilder.table import Table


class Schema:
    def __init__(self, name: str, tables: List[Table | str] | None = None):
        self.name = name
        if all(isinstance(table, str) for table in tables):
            tables = [Table(table_name, schema=self.name) for table_name in tables]
        self.tables = tables
