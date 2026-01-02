from abc import abstractmethod, ABC

from pysqlscribe.ast.base import Node
from pysqlscribe.ast.joins import JoinType
from pysqlscribe.exceptions import InvalidJoinException

SELECT = "SELECT"
FROM = "FROM"
WHERE = "WHERE"
LIMIT = "LIMIT"
JOIN = "JOIN"
ORDER_BY = "ORDER BY"
AND = "AND"
FETCH_NEXT = "FETCH NEXT"
OFFSET = "OFFSET"
GROUP_BY = "GROUP BY"
HAVING = "HAVING"
ALL = "ALL"
UNION = "UNION"
UNION_ALL = f"UNION {ALL}"
EXCEPT = "EXCEPT"
EXCEPT_ALL = f"EXCEPT {ALL}"
INTERSECT = "INTERSECT"
INTERSECT_ALL = f"INTERSECT {ALL}"
INSERT = "INSERT"
INTO = "INTO"
VALUES = "VALUES"


class SelectNode(Node):
    def __str__(self):
        return f"{SELECT} {self.state['columns']}"


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

    @property
    @abstractmethod
    def operation(self): ...

    @property
    def valid_next_nodes(self):
        return ()


class UnionNode(CombineNode):
    @property
    def operation(self):
        return UNION if not self.all else UNION_ALL


class ExceptNode(CombineNode):
    @property
    def operation(self):
        return EXCEPT if not self.all else EXCEPT_ALL


class IntersectNode(CombineNode):
    @property
    def operation(self):
        return INTERSECT if not self.all else INTERSECT_ALL


class InsertNode(Node):
    def __str__(self):
        if isinstance(self.state["values"], str):
            values = f"({self.state['values']})"
        elif isinstance(self.state["values"], list):
            values = ",".join([f"({v})" for v in self.state["values"]])
        else:
            raise ValueError(f"Invalid values: {self.state['values']}")
        columns = f" ({self.state['columns']})" if self.state["columns"] else ""
        return f"{INSERT} {INTO} {self.state['table']}{columns} {VALUES} {values}"


class ReturningNode(Node):
    def __str__(self):
        return f"RETURNING {self.state['columns']}"
