""" """

import re
from abc import abstractmethod, ABC
from typing import Any, Dict, Self, Callable, Sequence, Tuple

SELECT = "SELECT"
FROM = "FROM"
WHERE = "WHERE"
LIMIT = "LIMIT"
JOIN = "JOIN"
VALID_IDENTIFIER_REGEX = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def reconcile_args_into_string(*args) -> str:
    if len(args) == 1:
        arg = args[0]
        if isinstance(arg, str):
            fields = arg
        elif isinstance(arg, Sequence):
            fields = ",".join(arg)
        else:
            raise Exception("Invalid argument type")
    else:
        fields = ",".join(args)

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
        return JoinNode, WhereNode, GroupByNode, OrderByNode

    def __str__(self):
        return f"{FROM} {self.state['tables']}"


class WhereNode(Node):
    @property
    def valid_next_nodes(self):
        return JoinNode, GroupByNode, OrderByNode

    def __str__(self):
        return f"{WHERE} {self.state['condition']}"


class JoinNode(Node):
    @property
    def valid_next_nodes(self):
        return GroupByNode, OrderByNode

    def __str__(self):
        return f"{JOIN} {self.state['fields']}"


class GroupByNode(Node):
    @property
    def valid_next_nodes(self):
        return OrderByNode


class OrderByNode(Node):
    @property
    def valid_next_nodes(self):
        return LimitNode


class LimitNode(Node):
    def __str__(self):
        return f"{LIMIT} {self.state['condition']}"


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
        self.node.add(WhereNode({"conditions": args}))
        self.node = self.node.next_
        return self

    def build(self) -> str:
        node = self.node
        query = ""
        while True:
            query = str(node) + " " + query
            node = node.prev_
            if node is None:
                break
        return query.strip()


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
