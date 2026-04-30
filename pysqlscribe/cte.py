from typing import Any, Self

from pysqlscribe.exceptions import DuplicateCTENameError, EmptyCTEError
from pysqlscribe.params import ParamCollector
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
        self._subqueries: dict[str, str | Query] = {}

    def as_(self, subquery: str | Query) -> Self:
        self._subqueries[self._current_cte_name] = subquery
        return self

    def build(
        self, clear: bool = True, *, parameterize: bool = False
    ) -> str | tuple[str, list[Any]]:
        if not self._subqueries:
            raise EmptyCTEError(
                f"No subqueries defined for WITH clause '{self._current_cte_name}'"
            )
        with_block = f"{WITH if not self.recursive else WITH_RECURSIVE}"
        if parameterize:
            collector = ParamCollector(self.dialect)
            cte_queries = ", ".join(
                f"{name} {AS} ({self._render_subquery(sub, collector)})"
                for name, sub in self._subqueries.items()
            )
            outer = self.dialect.render(self.node, collector)
            if clear:
                self.node = None
            return f"{with_block} {cte_queries} {outer}".strip(), collector.params
        cte_queries = ", ".join(
            f"{name} {AS} ({self._render_subquery(sub, None)})"
            for name, sub in self._subqueries.items()
        )
        outer = super().build(clear=clear)
        return f"{with_block} {cte_queries} {outer}"

    @staticmethod
    def _render_subquery(subquery, collector: ParamCollector | None) -> str:
        if isinstance(subquery, Query):
            if collector is not None:
                return subquery.dialect.render(subquery.node, collector)
            return str(subquery)
        return subquery

    def with_(self, cte_name: str) -> Self:
        if cte_name in self._subqueries:
            raise DuplicateCTENameError(
                f"CTE with name '{cte_name}' already exists in this context"
            )
        self._current_cte_name = cte_name
        return self


def with_(cte_name: str, dialect: str, recursive: bool = False) -> With:
    return With(cte_name, dialect, recursive)
