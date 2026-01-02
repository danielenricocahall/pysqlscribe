from pysqlscribe.dialects.base import Dialect, DialectRegistry


@DialectRegistry.register("sqlite")
class SQLiteDialect(Dialect):
    def _escape_identifier(self, identifier: str) -> str:
        return f'"{identifier}"'
