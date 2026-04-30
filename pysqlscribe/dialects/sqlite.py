from pysqlscribe.dialects.base import Dialect, DialectRegistry
from pysqlscribe.renderers.base import Renderer
from pysqlscribe.renderers.sqlite import SqliteRenderer


@DialectRegistry.register("sqlite")
class SQLiteDialect(Dialect):
    def make_renderer(self) -> Renderer:
        return SqliteRenderer(self)

    def make_placeholder(self, index: int) -> str:
        return "?"

    def _escape_identifier(self, identifier: str) -> str:
        return f'"{identifier}"'

    def escape_value(self, value) -> str:
        # SQLite has no native boolean type and stores booleans as the integers
        # 1 / 0, so render them that way for the inline path. (Must precede
        # the int branch in the base class.)
        if isinstance(value, bool):
            return "1" if value else "0"
        return super().escape_value(value)
