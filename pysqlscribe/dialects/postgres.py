from pysqlscribe.dialects.base import Dialect


class PostgreSQLDialect(Dialect):
    def _escape_identifier(self, identifier: str) -> str:
        return f'"{identifier}"'
