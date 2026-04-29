from pysqlscribe.dialects.base import Dialect, DialectRegistry
from pysqlscribe.renderers.mysql import Renderer, MySQLRenderer


@DialectRegistry.register("mysql")
class MySQLDialect(Dialect):
    def make_renderer(self) -> Renderer:
        return MySQLRenderer(self)

    def make_placeholder(self, index: int) -> str:
        return "%s"

    def _escape_identifier(self, identifier: str) -> str:
        return f"`{identifier}`"

    def escape_value(self, value) -> str:
        # MySQL processes backslash escapes in string literals by default,
        # so both backslashes and single quotes must be escaped.
        if isinstance(value, str):
            escaped = value.replace("\\", "\\\\").replace("'", "''")
            return f"'{escaped}'"
        return super().escape_value(value)
