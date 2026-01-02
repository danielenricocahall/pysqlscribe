from pysqlscribe.ast.nodes import OffsetNode, FetchNextNode
from pysqlscribe.dialects.base import Dialect, DialectRegistry


@DialectRegistry.register("oracle")
class OracleDialect(Dialect):
    @property
    def valid_node_transitions(self):
        transitions = dict(super().valid_node_transitions)
        transitions[OffsetNode] = (FetchNextNode,)
        transitions[FetchNextNode] = tuple()
        return transitions

    def _escape_identifier(self, identifier: str) -> str:
        return f'"{identifier}"'
