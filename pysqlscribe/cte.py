from typing import Self

from pysqlscribe.exceptions import DuplicateCTENameException, EmptyCTEException
from pysqlscribe.query import Query

WITH = "WITH"
WITH_RECURSIVE = f"{WITH} RECURSIVE"
AS = "AS"


class With(Query):
    """Builder for WITH ... AS ... expressions."""

    def __init__(self, cte_name: str, dialect: str, recursive: bool = False):
        super().__init__(dialect)
        self._current_cte_name = cte_name
        self.recursive = recursive
        self._subqueries = {}

    def as_(self, subquery: str | Query) -> Self:
        if isinstance(subquery, Query):
            subquery = str(subquery)
        self._subqueries[self._current_cte_name] = subquery
        return self

    def build(self, clear: bool = True) -> str:
        if not self._subqueries:
            raise EmptyCTEException(
                f"No subqueries defined for WITH clause '{self._current_cte_name}'"
            )
        query = super().build(clear=clear)
        with_block = f"{WITH if not self.recursive else WITH_RECURSIVE}"
        cte_queries = ", ".join(
            f"{cte_name} {AS} ({subquery})"
            for cte_name, subquery in self._subqueries.items()
        )
        return f"{with_block} {cte_queries} {query}"

    def with_(self, cte_name: str) -> Self:
        if cte_name in self._subqueries:
            raise DuplicateCTENameException(
                f"CTE with name '{cte_name}' already exists in this context"
            )
        self._current_cte_name = cte_name
        return self


def with_(cte_name: str, dialect: str, recursive: bool = False) -> With:
    return With(cte_name, dialect, recursive)
