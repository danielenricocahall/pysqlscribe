from abc import ABC

from pysqlscribe.ast.base import Node
from pysqlscribe.ast.joins import JoinType
from pysqlscribe.exceptions import InvalidJoinException

AND = "AND"


class SelectNode(Node): ...


class FromNode(Node): ...


class JoinNode(Node):
    def __init__(self, state):
        super().__init__(state)
        self.join_type = state.get("join_type", JoinType.INNER)
        self.table = state["table"]
        if (condition := state.get("condition")) and self.join_type in (
            JoinType.NATURAL,
            JoinType.CROSS,
        ):
            raise InvalidJoinException(
                "Conditions need to be supplied for any join which is not NATURAL or CROSS"
            )
        self.condition = condition


class WhereNode(Node):
    def __and__(self, other):
        if isinstance(other, WhereNode):
            compound_condition = (
                f"{self.state['conditions']} {AND} {other.state['conditions']}"
            )
            return WhereNode({"conditions": compound_condition})


class OrderByNode(Node): ...


class LimitNode(Node): ...


class OffsetNode(Node): ...


class GroupByNode(Node): ...


class HavingNode(Node):
    def __and__(self, other):
        if isinstance(other, HavingNode):
            compound_condition = (
                f"{self.state['conditions']} {AND} {other.state['conditions']}"
            )
            return HavingNode({"conditions": compound_condition})


class CombineNode(Node, ABC):
    def __init__(self, state):
        super().__init__(state)
        self.query = state["query"]
        self.all = state.get("all", False)


class UnionNode(CombineNode): ...


class ExceptNode(CombineNode): ...


class IntersectNode(CombineNode): ...


class InsertNode(Node): ...


class ReturningNode(Node): ...
