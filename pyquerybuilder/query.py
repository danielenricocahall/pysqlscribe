import operator
from abc import abstractmethod, ABC
from functools import reduce
from typing import Any, Dict, Self, Callable, Sequence, Tuple

from pyquerybuilder.regex_patterns import VALID_IDENTIFIER_REGEX

SELECT = "SELECT"
FROM = "FROM"
WHERE = "WHERE"
LIMIT = "LIMIT"
ORDER_BY = "ORDER BY"
AND = "AND"


def reconcile_args_into_string(*args) -> str:
    arg = args[0]
    if isinstance(arg, str):
        fields = arg
    elif isinstance(arg, Sequence):
        fields = ",".join(arg)
    else:
        raise Exception("Invalid argument type")
    for field in fields.split(","):
        field = field.strip()
        if not VALID_IDENTIFIER_REGEX.match(field):
            raise ValueError(f"Invalid SQL identifier: {field}")
    return fields


class InvalidNodeException(Exception): ...


class Node(ABC):
    next_: Self | None = None
    prev_: Self | None = None
    state: dict[str, Any]

    def __init__(self, state):
        self.state = state

    def add(self, next_: "Node"):
        if not isinstance(next_, self.valid_next_nodes):
            raise InvalidNodeException()
        next_.prev_ = self
        self.next_ = next_

    @property
    @abstractmethod
    def valid_next_nodes(self) -> Tuple[type[Self], ...]: ...


class SelectNode(Node):
    @property
    def valid_next_nodes(self):
        return (FromNode,)

    def __str__(self):
        return f"{SELECT} {self.state['fields']}"


class FromNode(Node):
    @property
    def valid_next_nodes(self):
        return WhereNode, GroupByNode, OrderByNode, LimitNode

    def __str__(self):
        return f"{FROM} {self.state['tables']}"


class WhereNode(Node):
    @property
    def valid_next_nodes(self):
        return GroupByNode, OrderByNode, LimitNode, WhereNode

    def __str__(self):
        return f"{WHERE} {self.state['conditions']}"

    def __and__(self, other):
        if isinstance(other, WhereNode):
            compound_condition = (
                f"{self.state['conditions']} {AND} {other.state['conditions']}"
            )
            return WhereNode({"conditions": compound_condition})


class GroupByNode(Node):
    @property
    def valid_next_nodes(self):
        return OrderByNode


class OrderByNode(Node):
    @property
    def valid_next_nodes(self):
        return LimitNode

    def __str__(self):
        return f"{ORDER_BY} {self.state['fields']}"


class LimitNode(Node):
    @property
    def valid_next_nodes(self):
        return ()

    def __str__(self):
        return f"{LIMIT} {self.state['limit']}"


class FetchNextNode(LimitNode):
    def __str__(self):
        return f"FETCH NEXT {self.state['limit']} ROWS ONLY"


class Query(ABC):
    node: Node | None = None

    def select(self, *args) -> Self:
        if not self.node:
            self.node = SelectNode({"fields": reconcile_args_into_string(args)})
        return self

    def from_(self, *args) -> Self:
        self.node.add(FromNode({"tables": reconcile_args_into_string(args)}))
        self.node = self.node.next_
        return self

    def where(self, *args) -> Self:
        where_node = reduce(
            operator.and_, map(lambda arg: WhereNode({"conditions": arg}), args)
        )
        self.node.add(where_node)
        self.node = self.node.next_
        return self

    def order_by(self, *args) -> Self:
        self.node.add(OrderByNode({"fields": reconcile_args_into_string(args)}))
        self.node = self.node.next_
        return self

    def limit(self, n: int | str):
        self.node.add(LimitNode({"limit": int(n)}))
        self.node = self.node = self.node.next_
        return self

    def build(self, clear: bool = True) -> str:
        node = self.node
        query = ""
        while True:
            query = str(node) + " " + query
            node = node.prev_
            if node is None:
                break
        if clear:
            # we provide an option to not clear the builder in the event the developer needs
            # to debug or needs to reuse the value. By default, we do immediately after building the query
            self.node = None
        return query.strip()

    def __str__(self):
        return self.build(clear=False)


class QueryRegistry:
    builders: Dict[str, Query] = {}

    @classmethod
    def register(cls, key: str):
        def decorator(builder_class: Callable[[], Query]) -> Callable[[], Query]:
            cls.builders[key] = builder_class()
            return builder_class

        return decorator

    @classmethod
    def get_builder(cls, key: str) -> Query:
        return cls.builders[key]


@QueryRegistry.register("mysql")
class MySQLQuery(Query): ...


@QueryRegistry.register("oracle")
class OracleQuery(Query):
    def limit(self, n: int | str):
        self.node.add(FetchNextNode({"limit": int(n)}))
        self.node = self.node = self.node.next_
        return self
