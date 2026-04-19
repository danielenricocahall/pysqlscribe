from typing import Self

from pysqlscribe.query import Query

WITH = "WITH"
WITH_RECURSIVE = f"{WITH} RECURSIVE"
AS = "AS"


class With(Query):
    """Builder for WITH ... AS ... expressions."""

    def __init__(self, cte_name: str, dialect: str = None, recursive: bool = False):
        super().__init__(dialect)
        self._cte_name = cte_name
        self.recursive = recursive

    def as_(self, subquery: str | Query) -> Self:
        if isinstance(subquery, Query):
            subquery = str(subquery)
        self._subquery = f" {AS} ({subquery})"
        return self

    def build(self, clear: bool = True) -> str:
        query = super().build(clear=clear)
        cte_query = f"{WITH if not self.recursive else WITH_RECURSIVE} {self._cte_name}{self._subquery} {query}"
        return cte_query


def with_(cte_name: str, dialect: str = None, recursive: bool = False):
    return With(cte_name, dialect, recursive)
