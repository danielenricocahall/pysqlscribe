from pysqlscribe.dialects.base import Dialect, DialectRegistry
from pysqlscribe.renderers.sqlite import SqliteRenderer


@DialectRegistry.register("sqlite")
class SQLiteDialect(Dialect):
    def __init__(self):
        self._renderer = SqliteRenderer()

    def _escape_identifier(self, identifier: str) -> str:
        return f'"{identifier}"'
