from typing import Dict, Callable

from pysqlscribe.ast.base import Node
from pysqlscribe.ast.joins import JoinType
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
    SelectNode,
    InsertNode,
    ReturningNode,
)

SELECT = "SELECT"
FROM = "FROM"
WHERE = "WHERE"
LIMIT = "LIMIT"
JOIN = "JOIN"
ORDER_BY = "ORDER BY"
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
RETURNING = "RETURNING"


class Renderer:
    @property
    def dispatch(self) -> Dict[type[Node], Callable[[Node], str]]:
        return {
            SelectNode: self.render_select,
            FromNode: self.render_from,
            WhereNode: self.render_where,
            GroupByNode: self.render_group_by,
            OrderByNode: self.render_order_by,
            LimitNode: self.render_limit,
            JoinNode: self.render_join,
            HavingNode: self.render_having,
            OffsetNode: self.render_offset,
            UnionNode: self.render_union,
            ExceptNode: self.render_except,
            IntersectNode: self.render_intersect,
            InsertNode: self.render_insert,
            ReturningNode: self.render_returning,
        }

    def render(self, node: Node) -> str:
        query = ""
        while True:
            render_method = self.dispatch.get(type(node))
            query = render_method(node) + " " + query
            node = node.prev_
            if node is None:
                break
        return query.strip()

    def render_select(self, node: SelectNode) -> str:
        return f"{SELECT} {node.state['columns']}"

    def render_from(self, node: FromNode) -> str:
        return f"{FROM} {node.state['tables']}"

    def render_where(self, node: WhereNode) -> str:
        return f"{WHERE} {node.state['conditions']}"

    def render_group_by(self, node: GroupByNode) -> str:
        return f"{GROUP_BY} {node.state['columns']}"

    def render_order_by(self, node: OrderByNode) -> str:
        return f"{ORDER_BY} {node.state['columns']}"

    def render_limit(self, node: LimitNode) -> str:
        return f"{LIMIT} {node.state['limit']}"

    def render_join(self, node: JoinNode) -> str:
        return f"{node.join_type} {JOIN} {node.table} " + (
            f"ON {node.condition}"
            if node.join_type not in (JoinType.NATURAL, JoinType.CROSS)
            else ""
        )

    def render_having(self, node: HavingNode) -> str:
        return f"{HAVING} {node.state['conditions']}"

    def render_offset(self, node: OffsetNode) -> str:
        return f"{OFFSET} {node.state['offset']}"

    def render_union(self, node: UnionNode) -> str:
        operation = UNION_ALL if node.state.get("all", False) else UNION
        return f"{operation} {node.query}"

    def render_except(self, node: ExceptNode) -> str:
        operation = EXCEPT_ALL if node.state.get("all", False) else EXCEPT
        return f"{operation} {node.query}"

    def render_intersect(self, node: IntersectNode) -> str:
        operation = INTERSECT_ALL if node.state.get("all", False) else INTERSECT
        return f"{operation} {node.query}"

    def render_insert(self, node: InsertNode) -> str:
        if isinstance(node.state["values"], str):
            values = f"({node.state['values']})"
        elif isinstance(node.state["values"], list):
            values = ",".join([f"({v})" for v in node.state["values"]])
        else:
            raise ValueError(f"Invalid values: {node.state['values']}")
        columns = f" ({node.state['columns']})" if node.state["columns"] else ""
        return f"{INSERT} {INTO} {node.state['table']}{columns} {VALUES} {values}"

    def render_returning(self, node: ReturningNode) -> str:
        return f"{RETURNING} {node.state['columns']}"
