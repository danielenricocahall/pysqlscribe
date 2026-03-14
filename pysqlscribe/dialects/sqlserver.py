from pysqlscribe.ast.nodes import OffsetNode, LimitNode, OrderByNode
from pysqlscribe.dialects.base import Dialect, DialectRegistry
from pysqlscribe.renderers.base import Renderer
from pysqlscribe.renderers.sqlserver import SQLServerRenderer


@DialectRegistry.register("sqlserver")
class SQLServerDialect(Dialect):
    def make_renderer(self) -> Renderer:
        return SQLServerRenderer(self)

    @property
    def valid_node_transitions(self):
        transitions = dict(super().valid_node_transitions)
        transitions[OrderByNode] = (OffsetNode,)
        transitions[OffsetNode] = (LimitNode,)
        transitions[LimitNode] = tuple()
        return transitions

    def _escape_identifier(self, identifier: str) -> str:
        return f"[{identifier}]"
