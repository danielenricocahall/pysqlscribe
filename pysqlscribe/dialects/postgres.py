from pysqlscribe.dialects.base import Dialect, DialectRegistry
from pysqlscribe.renderers.base import Renderer
from pysqlscribe.renderers.postgres import PostgresRenderer


@DialectRegistry.register("postgres")
class PostgreSQLDialect(Dialect):
    def make_renderer(self) -> Renderer:
        return PostgresRenderer()

    def _escape_identifier(self, identifier: str) -> str:
        return f'"{identifier}"'
