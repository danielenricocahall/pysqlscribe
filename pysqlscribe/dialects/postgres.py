from pysqlscribe.dialects.base import Dialect, DialectRegistry


@DialectRegistry.register("postgres")
class PostgreSQLDialect(Dialect):
    def _escape_identifier(self, identifier: str) -> str:
        return f'"{identifier}"'
