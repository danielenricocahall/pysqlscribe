from pysqlscribe.dialects.base import Dialect, DialectRegistry
from pysqlscribe.renderers.mysql import Renderer, MySQLRenderer


@DialectRegistry.register("mysql")
class MySQLDialect(Dialect):
    def make_renderer(self) -> Renderer:
        return MySQLRenderer()

    def _escape_identifier(self, identifier: str) -> str:
        return f"`{identifier}`"
