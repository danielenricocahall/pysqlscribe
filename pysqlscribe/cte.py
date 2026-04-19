from typing import Self

from pysqlscribe.alias import AliasMixin
from pysqlscribe.query import Query

WITH = "WITH"
WITH_RECURSIVE = f"{WITH} RECURSIVE"


class With(Query, AliasMixin):
    """Builder for WITH ... AS ... expressions."""

    def __init__(self, cte_name: str, dialect: str = None, recursive: bool = False):
        super().__init__(dialect)
        self._cte_name = cte_name
        self.recursive = recursive

    def as_(self, alias: str | Query) -> Self:
        if isinstance(alias, Query):
            alias = str(alias)
        return super().as_(f"({alias})")

    def build(self, clear: bool = True) -> str:
        query = super().build(clear=clear)
        cte_query = f"{WITH if not self.recursive else WITH_RECURSIVE} {self._cte_name}{self.alias} {query}"
        return cte_query
