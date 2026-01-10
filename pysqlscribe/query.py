from abc import ABC
from typing import Dict, Self

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
    InsertNode,
    ReturningNode,
    UnionNode,
    LimitNode,
)
from pysqlscribe.dialects import (
    Dialect,
)
from pysqlscribe.dialects.base import DialectRegistry


class Query(ABC):
    node: Node | None = None
    _dialect_key: str

    def __init__(self):
        self._dialect = DialectRegistry.get_dialect(self._dialect_key)

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

    def insert(self, *columns, **kwargs) -> Self:
        table = kwargs.get("into")
        values = kwargs.get("values")
        if table is None or values is None:
            raise ValueError("Insert queries require `into` and `values` keywords.")
        if not self.node:
            self.node = InsertNode(
                {"columns": columns, "table": table, "values": values}
            )
        return self

    def returning(self, *args) -> Self:
        self.node.add(ReturningNode({"columns": list(args)}), self.dialect)
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

    def inner_join(self, table: str, condition: str):
        return self.join(table, JoinType.INNER, condition)

    def outer_join(self, table: str, condition: str):
        return self.join(table, JoinType.OUTER, condition)

    def left_join(self, table: str, condition: str):
        return self.join(table, JoinType.LEFT, condition)

    def right_join(self, table: str, condition: str):
        return self.join(table, JoinType.RIGHT, condition)

    def cross_join(self, table: str):
        return self.join(table, JoinType.CROSS)

    def natural_join(self, table: str):
        return self.join(table, JoinType.NATURAL)

    def where(self, *args) -> Self:
        where_node = WhereNode({"conditions": list(args)})
        self.node.add(where_node, self.dialect)
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
        having_node = HavingNode({"conditions": list(args)})
        self.node.add(having_node, self.dialect)
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


class QueryRegistry:
    builders: Dict[str, type[Query]] = {}

    @classmethod
    def register(cls, key: str):
        def decorator(builder_class: type[Query]) -> type[Query]:
            cls.builders[key] = builder_class
            return builder_class

        return decorator

    @classmethod
    def get_builder(cls, key: str) -> Query:
        return cls.builders[key]()


@QueryRegistry.register("mysql")
class MySQLQuery(Query):
    _dialect_key = "mysql"


@QueryRegistry.register("oracle")
class OracleQuery(Query):
    _dialect_key = "oracle"


@QueryRegistry.register("postgres")
class PostgreSQLQuery(Query):
    _dialect_key = "postgres"


@QueryRegistry.register("sqlite")
class SQLiteQuery(Query):
    _dialect_key = "sqlite"
