from pysqlscribe.ast.nodes import OffsetNode, LimitNode, OrderByNode
from pysqlscribe.dialects.base import Dialect, DialectRegistry
from pysqlscribe.renderers.oracle import OracleRenderer


@DialectRegistry.register("oracle")
class OracleDialect(Dialect):
    def __init__(self):
        self._renderer = OracleRenderer()

    @property
    def valid_node_transitions(self):
        transitions = dict(super().valid_node_transitions)
        transitions[OrderByNode] = (OffsetNode,)
        transitions[OffsetNode] = (LimitNode,)
        transitions[LimitNode] = tuple()
        return transitions

    def _escape_identifier(self, identifier: str) -> str:
        return f'"{identifier}"'
