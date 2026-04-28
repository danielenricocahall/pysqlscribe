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
)
from pysqlscribe.column import OrderedColumn, Expression
from pysqlscribe.params import ParamCollector
from pysqlscribe.protocols import DialectProtocol
from pysqlscribe.regex_patterns import WILDCARD_REGEX

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
AND = "AND"


class Renderer:
    def __init__(self, dialect: DialectProtocol):
        self.dialect = dialect

    @property
    def dispatch(
        self,
    ) -> Dict[type[Node], Callable[[Node, ParamCollector | None], str]]:
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
        }

    def render(self, node: Node, collector: ParamCollector | None = None) -> str:
        head = node
        while head.prev_ is not None:
            head = head.prev_
        parts = []
        cur = head
        while cur is not None:
            parts.append(self.dispatch[type(cur)](cur, collector))
            cur = cur.next_
        return " ".join(parts).strip()

    def render_select(self, node: SelectNode, collector: ParamCollector | None) -> str:
        columns = self._resolve_columns(*node.state["columns"], collector=collector)
        return f"{SELECT} {columns}"

    def render_from(self, node: FromNode, collector: ParamCollector | None) -> str:
        tables = self.dialect.normalize_identifiers_args(
            node.state["tables"], collector=collector
        )
        return f"{FROM} {tables}"

    def render_where(self, node: WhereNode, collector: ParamCollector | None) -> str:
        conditions = f" {AND} ".join(
            self._render_condition(c, collector) for c in node.state["conditions"]
        )
        return f"{WHERE} {conditions}"

    def _render_condition(self, condition, collector: ParamCollector | None) -> str:
        if isinstance(condition, Expression):
            return condition.render(collector)
        return str(condition)

    def render_group_by(
        self, node: GroupByNode, collector: ParamCollector | None
    ) -> str:
        columns = self.dialect.normalize_identifiers_args(
            node.state["columns"], collector=collector
        )
        return f"{GROUP_BY} {columns}"

    def render_order_by(
        self, node: OrderByNode, collector: ParamCollector | None
    ) -> str:
        parts = []
        for col in node.state["columns"]:
            if isinstance(col, OrderedColumn):
                escaped = self.dialect.normalize_identifiers_args(
                    [col.name], collector=collector
                )
                parts.append(f"{escaped} {col.direction}")
            else:
                parts.append(
                    self.dialect.normalize_identifiers_args([col], collector=collector)
                )
        return f"{ORDER_BY} {', '.join(parts)}"

    def render_limit(self, node: LimitNode, collector: ParamCollector | None) -> str:
        return f"{LIMIT} {node.state['limit']}"

    def render_join(self, node: JoinNode, collector: ParamCollector | None) -> str:
        table = self.dialect.normalize_identifiers_args(node.table, collector=collector)
        return f"{node.join_type} {JOIN} {table} " + (
            f"ON {self._render_condition(node.condition, collector)}"
            if node.join_type not in (JoinType.NATURAL, JoinType.CROSS)
            else ""
        )

    def render_having(self, node: HavingNode, collector: ParamCollector | None) -> str:
        conditions = f" {AND} ".join(
            self._render_condition(c, collector) for c in node.state["conditions"]
        )
        return f"{HAVING} {conditions}"

    def render_offset(self, node: OffsetNode, collector: ParamCollector | None) -> str:
        return f"{OFFSET} {node.state['offset']}"

    def _render_combine_query(self, query, collector: ParamCollector | None) -> str:
        if (
            collector is not None
            and hasattr(query, "node")
            and hasattr(query, "dialect")
        ):
            return query.dialect.render(query.node, collector)
        return str(query)

    def render_union(self, node: UnionNode, collector: ParamCollector | None) -> str:
        operation = UNION_ALL if node.state.get("all", False) else UNION
        return f"{operation} {self._render_combine_query(node.query, collector)}"

    def render_except(self, node: ExceptNode, collector: ParamCollector | None) -> str:
        operation = EXCEPT_ALL if node.state.get("all", False) else EXCEPT
        return f"{operation} {self._render_combine_query(node.query, collector)}"

    def render_intersect(
        self, node: IntersectNode, collector: ParamCollector | None
    ) -> str:
        operation = INTERSECT_ALL if node.state.get("all", False) else INTERSECT
        return f"{operation} {self._render_combine_query(node.query, collector)}"

    def _resolve_columns(self, *args, collector: ParamCollector | None = None) -> str:
        if not args:
            args = ["*"]
        if isinstance(args[0], str) and WILDCARD_REGEX.match(args[0]):
            columns = args[0]
        else:
            columns = self.dialect.normalize_identifiers_args(args, collector=collector)
        return columns
