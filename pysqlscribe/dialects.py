from abc import ABC

from pysqlscribe.ast.base import Node
from pysqlscribe.ast.nodes import (
    FromNode,
    JoinNode,
    WhereNode,
    GroupByNode,
    OrderByNode,
    LimitNode,
    UnionNode,
    ExceptNode,
    IntersectNode,
    HavingNode,
    OffsetNode,
    FetchNextNode,
    SelectNode,
    InsertNode,
    ReturningNode,
)
from pysqlscribe.exceptions import DialectValidationError


class Dialect(ABC):
    def validate(self, current_node: Node, next_node: Node):
        valid_next = self.valid_node_transitions.get(type(current_node), ())
        if not isinstance(next_node, valid_next):
            raise DialectValidationError(
                f"{type(next_node).__name__} cannot follow {type(current_node).__name__}"
            )

    @property
    def valid_node_transitions(self) -> dict[type[Node], tuple[type[Node], ...]]:
        return {
            SelectNode: (FromNode,),
            FromNode: (
                JoinNode,
                WhereNode,
                GroupByNode,
                OrderByNode,
                LimitNode,
                UnionNode,
                ExceptNode,
                IntersectNode,
                OffsetNode,
            ),
            JoinNode: (WhereNode, GroupByNode, OrderByNode, LimitNode, JoinNode),
            WhereNode: (WhereNode, GroupByNode, OrderByNode, LimitNode),
            OrderByNode: (LimitNode,),
            LimitNode: (OffsetNode,),
            GroupByNode: (HavingNode, OrderByNode, LimitNode),
            HavingNode: (OrderByNode, LimitNode),
            UnionNode: (),
            ExceptNode: (),
            IntersectNode: (),
            InsertNode: (ReturningNode,),
        }


class PostgreSQLDialect(Dialect): ...


class MySQLDialect(Dialect): ...


class OracleDialect(Dialect):
    @property
    def valid_node_transitions(self):
        transitions = dict(super().valid_node_transitions)
        transitions[OffsetNode] = (FetchNextNode,)
        transitions[FetchNextNode] = tuple()
        return transitions


class SQLiteDialect(Dialect): ...
