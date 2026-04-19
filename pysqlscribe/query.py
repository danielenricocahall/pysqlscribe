from typing import Self

from pysqlscribe.alias import AliasMixin
from pysqlscribe.ast.base import Node
from pysqlscribe.ast.joins import JoinType
from pysqlscribe.ast.nodes import (
    SelectNode,
    FromNode,
    JoinNode,
    GroupByNode,
    OffsetNode,
    IntersectNode,
    ExceptNode,
    HavingNode,
    OrderByNode,
    WhereNode,
    UnionNode,
    LimitNode,
)
from pysqlscribe.dialects import (
    Dialect,
)
from pysqlscribe.dialects.base import DialectRegistry


class Query(AliasMixin):
    node: Node | None = None

    def __init__(self, dialect: str):
        if dialect not in DialectRegistry.dialects:
            raise ValueError(f"Unsupported dialect: {dialect}")
        self._dialect = DialectRegistry.get_dialect(dialect)

    @property
    def dialect(self) -> Dialect:
        return self._dialect

    def select(self, *args) -> Self:
        if not self.node:
            self.node = SelectNode({"columns": list(args)})
        return self

    def from_(self, *args) -> Self:
        self.node.add(
            FromNode({"tables": list(args)}),
            self.dialect,
        )
        self.node = self.node.next_
        return self

    def join(
        self, table: str, join_type: str = JoinType.INNER, condition: str | None = None
    ) -> Self:
        self.node.add(
            JoinNode(
                {
                    "join_type": join_type.upper(),
                    "table": table,
                    "condition": condition,
                }
            ),
            self.dialect,
        )
        self.node = self.node.next_
        return self

    def inner_join(self, table: str, condition: str) -> Self:
        return self.join(table, JoinType.INNER, condition)

    def outer_join(self, table: str, condition: str) -> Self:
        return self.join(table, JoinType.OUTER, condition)

    def left_join(self, table: str, condition: str) -> Self:
        return self.join(table, JoinType.LEFT, condition)

    def right_join(self, table: str, condition: str) -> Self:
        return self.join(table, JoinType.RIGHT, condition)

    def cross_join(self, table: str) -> Self:
        return self.join(table, JoinType.CROSS)

    def natural_join(self, table: str) -> Self:
        return self.join(table, JoinType.NATURAL)

    def where(self, *args) -> Self:
        if isinstance(self.node, WhereNode):
            self.node.state["conditions"].extend(args)
        else:
            self.node.add(WhereNode({"conditions": list(args)}), self.dialect)
            self.node = self.node.next_
        return self

    def order_by(self, *args) -> Self:
        self.node.add(
            OrderByNode({"columns": list(args)}),
            self.dialect,
        )
        self.node = self.node.next_
        return self

    def limit(self, n: int | str):
        self.node.add(LimitNode({"limit": int(n)}), self.dialect)
        self.node = self.node.next_
        return self

    def offset(self, n: int | str):
        self.node.add(OffsetNode({"offset": int(n)}), self.dialect)
        self.node = self.node.next_
        return self

    def group_by(self, *args) -> Self:
        self.node.add(
            GroupByNode({"columns": list(args)}),
            self.dialect,
        )
        self.node = self.node.next_
        return self

    def having(self, *args) -> Self:
        if isinstance(self.node, HavingNode):
            self.node.state["conditions"].extend(args)
        else:
            self.node.add(HavingNode({"conditions": list(args)}), self.dialect)
            self.node = self.node.next_
        return self

    def union(self, query: Self | str, all_: bool = False) -> Self:
        self.node.add(UnionNode({"query": query, "all": all_}), self.dialect)
        self.node = self.node.next_
        return self

    def except_(self, query: Self | str, all_: bool = False) -> Self:
        self.node.add(ExceptNode({"query": query, "all": all_}), self.dialect)
        self.node = self.node.next_
        return self

    def intersect(self, query: Self | str, all_: bool = False) -> Self:
        self.node.add(IntersectNode({"query": query, "all": all_}), self.dialect)
        self.node = self.node.next_
        return self

    def build(self, clear: bool = True) -> str:
        query = self.dialect.render(self.node)
        if clear:
            self.node = None
        return query.strip()

    def __str__(self):
        return self.build(clear=False)

    def disable_escape_identifiers(self):
        self.dialect.escape_identifiers_enabled = False
        return self

    def enable_escape_identifiers(self):
        self.dialect.escape_identifiers_enabled = True
        return self
