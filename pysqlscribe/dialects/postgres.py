from pysqlscribe.ast.nodes import LimitNode, OffsetNode
from pysqlscribe.dialects.base import Dialect, DialectRegistry
from pysqlscribe.renderers.base import Renderer
from pysqlscribe.renderers.postgres import PostgresRenderer


@DialectRegistry.register("postgres")
class PostgreSQLDialect(Dialect):
    def make_renderer(self) -> Renderer:
        return PostgresRenderer(self)

    def make_placeholder(self, index: int) -> str:
        return f"${index}"

    @property
    def valid_node_transitions(self):
        # Postgres allows bare OFFSET (without LIMIT) wherever LIMIT is allowed.
        transitions = dict(super().valid_node_transitions)
        for node, successors in transitions.items():
            if LimitNode in successors and OffsetNode not in successors:
                transitions[node] = successors + (OffsetNode,)
        return transitions

    def _escape_identifier(self, identifier: str) -> str:
        return f'"{identifier}"'
