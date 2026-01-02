from pysqlscribe.dialects.base import Dialect


class MySQLDialect(Dialect):
    def _escape_identifier(self, identifier: str) -> str:
        return f"`{identifier}`"
