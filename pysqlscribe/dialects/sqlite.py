from pysqlscribe.dialects.base import Dialect


class SQLiteDialect(Dialect):
    def _escape_identifier(self, identifier: str) -> str:
        return f'"{identifier}"'
