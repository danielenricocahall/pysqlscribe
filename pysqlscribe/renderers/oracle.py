from pysqlscribe.ast.nodes import LimitNode, OffsetNode
from pysqlscribe.params import ParamCollector
from pysqlscribe.renderers.base import Renderer, OFFSET

FETCH_NEXT = "FETCH NEXT"


class OracleRenderer(Renderer):
    def render_limit(self, node: LimitNode, collector: ParamCollector | None) -> str:
        return f"{FETCH_NEXT} {node.state['limit']} ROWS ONLY"

    def render_offset(self, node: OffsetNode, collector: ParamCollector | None) -> str:
        return f"{OFFSET} {node.state['offset']} ROWS"
