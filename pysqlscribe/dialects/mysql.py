from pysqlscribe.dialects.base import Dialect, DialectRegistry
from pysqlscribe.renderers.mysql import MySQLRenderer


@DialectRegistry.register("mysql")
class MySQLDialect(Dialect):
    def __init__(self):
        self._renderer = MySQLRenderer()

    def _escape_identifier(self, identifier: str) -> str:
        return f"`{identifier}`"
