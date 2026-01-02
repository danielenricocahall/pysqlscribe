from pysqlscribe.dialects.base import Dialect, DialectRegistry
from pysqlscribe.renderers.base import Renderer
from pysqlscribe.renderers.sqlite import SqliteRenderer


@DialectRegistry.register("sqlite")
class SQLiteDialect(Dialect):
    def make_renderer(self) -> Renderer:
        return SqliteRenderer(self)

    def _escape_identifier(self, identifier: str) -> str:
        return f'"{identifier}"'
