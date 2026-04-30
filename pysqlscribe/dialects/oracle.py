from pysqlscribe.ast.nodes import OffsetNode, LimitNode
from pysqlscribe.dialects.base import Dialect, DialectRegistry
from pysqlscribe.renderers.base import Renderer
from pysqlscribe.renderers.oracle import OracleRenderer


@DialectRegistry.register("oracle")
class OracleDialect(Dialect):
    def make_renderer(self) -> Renderer:
        return OracleRenderer(self)

    def make_placeholder(self, index: int) -> str:
        return f":{index}"

    @property
    def valid_node_transitions(self):
        # Oracle's pagination is `[OFFSET n ROWS] FETCH NEXT m ROWS ONLY`, so
        # OFFSET is allowed anywhere LIMIT (rendered as FETCH) is, and OFFSET
        # may optionally be followed by LIMIT. Nothing follows LIMIT.
        transitions = dict(super().valid_node_transitions)
        for node, successors in transitions.items():
            if LimitNode in successors and OffsetNode not in successors:
                transitions[node] = successors + (OffsetNode,)
        transitions[OffsetNode] = (LimitNode,)
        transitions[LimitNode] = ()
        return transitions

    def _escape_identifier(self, identifier: str) -> str:
        return f'"{identifier}"'
