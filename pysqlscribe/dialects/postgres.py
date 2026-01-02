from pysqlscribe.dialects.base import Dialect, DialectRegistry
from pysqlscribe.renderers.postgres import PostgresRenderer


@DialectRegistry.register("postgres")
class PostgreSQLDialect(Dialect):
    def __init__(self):
        self._renderer = PostgresRenderer()

    def _escape_identifier(self, identifier: str) -> str:
        return f'"{identifier}"'
